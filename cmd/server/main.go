// KV Database Server
package main

import (
    "fmt"
    "os"
    "net"
    "errors"
    "strings"
    "strconv"
    "syscall"
    "os/signal"
    "github.com/marciobarbosa/url-shortener/pkg/log"
    "github.com/marciobarbosa/url-shortener/pkg/segment"
    "github.com/marciobarbosa/url-shortener/pkg/database"
)

// default ip address
var ipaddr string = "127.0.0.1"
var port string

// default log level
var loglevel string = "INFO"
var logname string = "logs.txt"

var cachesize int
var cachepolicy string

// default directory
var directory string = "."

// Parse options given by the user
//
// Parameters:
//   parms: provided options
func ParseCmd(parms []string) {
    parm_i := 0
    for {
	parm := parms[parm_i]
	nparams := len(parms) - parm_i

	switch parm {
	case "-a":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    ipaddr = parms[parm_i+1]
	    parm_i += 2
	case "-p":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    port = parms[parm_i+1]
	    parm_i += 2
	case "-d":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    directory = parms[parm_i+1]
	    parm_i += 2
	case "-ll":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    loglevel = parms[parm_i+1]
	    parm_i += 2
	case "-l":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    logname = parms[parm_i+1]
	    parm_i += 2
	case "-c":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    cachesize, _ = strconv.Atoi(parms[parm_i+1])
	    parm_i += 2
	case "-s":
	    if nparams < 2 {
		PrintHelp()
		os.Exit(1)
	    }
	    cachepolicy = parms[parm_i+1]
	    parm_i += 2
	case "-h":
	    PrintHelp()
	    os.Exit(1)
	    parm_i += 1
	default:
	    PrintHelp()
	    os.Exit(1)
	}
	if parm_i >= len(parms)-1 {
	    break
	}
    }
}

// Check if request has been refused.
//
// Parameters:
//   status: status received from the database layer.
//
// Returns:
//   message: error message, if any.
//   refused: true if request has been refused..
func _RequestRefused(stat database.Status) (string, bool) {
    var refused bool = true
    var message string = ""

    switch stat {
    case database.STOPPED:
	message = "server_stopped\r\n"
    case database.LOCKED:
	message = "server_write_lock\r\n"
    default:
	refused = false
    }
    return message, refused
}

// Parse and execute request: put, get, and delete.
//
// Parameters:
//   conn: connection to the client.
//   msg: request.
func ParseMessage(conn net.Conn, msg string) {
    if len(msg) == 0 {
	return
    }
    tokens := strings.Fields(msg)
    cmd := tokens[0]

    log.Log(msg, "ALL")

    switch cmd {
    case "put":
	if len(tokens) < 3 {
	    conn.Write([]byte("error: missing arguments\r\n"))
	    return
	}
	key := []byte(tokens[1])
	data := msg[4 + len(tokens[1]) + 1:]
	data = strings.TrimSuffix(data, "\r\n")
	value := []byte(data)

	stat := database.Insert(key, value)
	reply, refused := _RequestRefused(stat)
	if !refused {
	    switch stat {
	    case database.CREATED:
		reply = "put_success " + tokens[1] + "\r\n"
	    case database.UPDATED:
		reply = "put_update " + tokens[1] + "\r\n"
	    default:
		reply = "put_error\r\n"
	    }
	}
	log.Log(reply, "ALL")
	conn.Write([]byte(reply))
    case "get":
	if len(tokens) < 2 {
	    conn.Write([]byte("error: missing arguments\r\n"))
	    return
	}
	key := []byte(tokens[1])
	data, stat := database.Request(key)
	reply, refused := _RequestRefused(stat)
	if !refused {
	    switch stat {
	    case database.FOUND:
		reply = "get_success " + tokens[1] + " " + string(data) + "\r\n"
	    default:
		reply = "get_error " + tokens[1] + "\r\n"
	    }
	}
	log.Log(reply, "ALL")
	conn.Write([]byte(reply))
    case "delete":
	if len(tokens) < 2 {
	    conn.Write([]byte("error missing arguments\r\n"))
	    return
	}
	key := []byte(tokens[1])
	data, stat := database.Remove(key)
	reply, refused := _RequestRefused(stat)
	if !refused {
	    switch stat {
	    case database.DELETED:
		reply = "delete_success " + tokens[1] + " " + string(data) + "\r\n"
	    default:
		reply = "delete_error " + tokens[1] + "\r\n"
	    }
	}
	log.Log(reply, "ALL")
	conn.Write([]byte(reply))
    default:
	conn.Write([]byte("error command not supported\r\n"))
    }
}

// Handle requests from a specific client.
//
// Parameters:
//   conn: connection to the client.
func handleConnection(conn net.Conn) {
    buf := make([]byte, 1024 * 32)

    conn.Write([]byte("connect\r\n"))
    for {
	len, err := conn.Read(buf)
	if err != nil {
	    conn.Close()
	    return
	}
	ParseMessage(conn, string(buf[:len]))
    }
    conn.Close()
}

// Start the server and wait for connections/requests from clients.
func Start() {
    var params database.InitParams

    params.Policy = database.LFU
    params.CacheSize = cachesize
    params.BasePath = directory
    params.LogName = logname

    err := database.Init(params)
    if err != nil {
        panic(err)
    }
    inited := log.InitPath(directory, logname)
    if !inited {
	panic(errors.New("Could not create log file"))
    }
    set, _ := log.SetLevel(loglevel)
    if !set {
	panic(errors.New("Could not set log level"))
    }

    addr := ipaddr + ":" + port
    ln, err := net.Listen("tcp", addr)
    if err != nil {
	panic(err)
    }
    defer ln.Close()

    log.Log("Listening on host: " + ipaddr + ", port: " + port, "INFO")

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

    go func() {
	<-sigs
	segment.FlushCache()
	os.Exit(0)
    }()

    for {
	conn, err := ln.Accept()
	if err != nil {
	    panic(err)
	}
	go handleConnection(conn)
    }
}

// Print available options
func PrintHelp() {
    fmt.Println("-a <ip> -p <port> -d <directory> -l <log> -ll <loglevel>")
    fmt.Println("-c <cachesize> -s <cachepolicy> -h")
}

// Print provided options
func PrintParms() {
    fmt.Printf("addr: %v port: %v\n", ipaddr, port)
    fmt.Printf("directory: %v logname: %v loglevel: %v\n", directory, logname, loglevel)
    fmt.Printf("cachesize: %v cachepolicy: %v\n", cachesize, cachepolicy)
}

func main() {
    if len(os.Args[1:]) < 1 {
	PrintHelp()
	os.Exit(1)
    }
    ParseCmd(os.Args[1:])
    Start()
}
