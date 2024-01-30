// EchoClient
//
// Establishes a TCP connection to a given server and exchange messages with it.
package main

import (
    "fmt"
    "bufio"
    "os"
    "time"
    "strings"
    "github.com/marciobarbosa/url-shortener/pkg/log"
    "github.com/marciobarbosa/url-shortener/pkg/network"
)

var bkhost string = "172.20.0.20"
var bkport string = "43203"

var host = ""
var port = ""

var kv_conn = ""
var kv_servers []string

var cmdMessage = `
Usage: client <host>:<port>+  -- list of kv-store servers
`
var helpMessage = `
connect <address> <port> 	-- connect to the specified <address:port>
send <msg>			-- send message to the server
logLevel <level>		-- set the logger to the specified log level
disconnect 			-- disconnect from the connected server
help
quit
`

// Parse and execute request.
//
// Parameters:
//   args: request.
func execCmd(args []string) {
    cmd := args[0]

    switch cmd {
    case "connect":
	if len(args) < 3 {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(helpMessage)
	    return
	}
	host = args[1]
	port = args[2]

	connected, msg := network.Connect(host, port)
	if connected == false {
	    fmt.Println(msg)
	    log.Log(msg, "WARNING")
	    return;
	}
	log.Log(msg, "INFO")
	fmt.Println(msg)

    case "send":
	if len(args) < 2 {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(helpMessage)
	    return
	}
	for {
	    sent, msg := network.Send([]byte(strings.Join(args[1:], " ") + "\r"))
	    if sent == false {
		network.Disconnect()
		newconn := false
		for _, server := range kv_servers {
		    if server == kv_conn {
			continue
		    }
		    fmt.Printf("Trying to connect to: %s\n", server)
		    kv_host := strings.Split(server, ":")[0]
		    kv_port := strings.Split(server, ":")[1]
		    connected, _ := network.Connect(kv_host, kv_port)
		    if connected == true {
			fmt.Printf("Connected to: %s\n", server)
			newconn = true
			kv_conn = server
			break
		    }
		}
		if newconn == false {
		    fmt.Println(msg)
		    log.Log(msg, "WARNING")
		    return
		}
		continue
	    }
	    if strings.Contains(msg, "error: leader:") {
		leader := strings.Split(msg, ": ")[len(strings.Split(msg, ": ")) - 1]
		leader = strings.TrimSuffix(leader, "\r\n")

		fmt.Printf("Leader: %s\n", leader)

		network.Disconnect()
		leader_ip := strings.Split(leader, ":")[0]
		leader_port := strings.Split(leader, ":")[1]

		fmt.Printf("Connecting to leader: %s:%s\n", leader_ip, leader_port)

		connected, _ := network.Connect(leader_ip, leader_port)
		if connected == false {
		    fmt.Println("Retrying in 2 seconds...")
		    time.Sleep(2 * time.Second)
		    continue
		}
		fmt.Printf("Connected to leader: %s:%s\n", leader_ip, leader_port)
		continue
	    }
	    if msg != "server_write_lock\r\n" && msg != "error: no leader elected\r\n" {
		log.Log(msg, "INFO")
		fmt.Println(msg)
		break
	    }
	    fmt.Println("Retrying in 2 seconds...")
	    time.Sleep(2 * time.Second)
	}

    case "disconnect":
	msg := "Connection terminated: " + host + " / " + port
	fmt.Println(msg)
	log.Log(msg, "ALL")
	network.Disconnect()
	host = ""
	port = ""

    case "logLevel":
	if len(args) < 2 {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(helpMessage)
	    return
	}
	changed, msg := log.SetLevel(args[1])
	if changed == false {
	    fmt.Println(msg)
	    log.Log(msg, "WARNING")
	    return
	}
	fmt.Println(msg)
	log.Log(msg, "INFO")

    case "help":
	fmt.Println(helpMessage)

    case "quit":
	msg := "Application exit!"
	network.Disconnect()
	fmt.Println(msg)
	log.Log(msg, "ALL")
	log.Destroy()
	os.Exit(0)

    default:
	fmt.Println("Unknown command")
	fmt.Println(helpMessage)
    }
}

func main() {
    reader := bufio.NewReader(os.Stdin)
    log.Init()

    if len(os.Args) < 2 {
	fmt.Println("Error: Incorrect arguments")
	fmt.Println(cmdMessage)
	return
    }

    for i := 1; i < len(os.Args); i++ {
	server := os.Args[i]
	kv_servers = append(kv_servers, server)
	if strings.Contains(server, ":") == false {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(cmdMessage)
	    return
	}
    }
    for _, server := range kv_servers {
	kv_host := strings.Split(server, ":")[0]
	kv_port := strings.Split(server, ":")[1]

	connected, _ := network.Connect(kv_host, kv_port)
	if connected == true {
	    kv_conn = server
	    break
	}
    }

    for {
	fmt.Print("EchoClient> ")

	input, err := reader.ReadString('\n')
	if err != nil {
	    fmt.Fprintln(os.Stderr, err)
	}

	input = strings.TrimSuffix(input, "\n")
	args := strings.Split(input, " ")
	execCmd(args)
    }
}
