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
    "encoding/csv"
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
push <input.csv>		-- upload list of key-value pairs to the server
pull <input.csv> <output.csv>	-- download list of key-value pairs from the server
logLevel <level>		-- set the logger to the specified log level
disconnect 			-- disconnect from the connected server
help
quit
`

func ReConnect() bool {
    network.Disconnect()
    for _, server := range kv_servers {
	if server == kv_conn {
	    continue
	}
	kv_host := strings.Split(server, ":")[0]
	kv_port := strings.Split(server, ":")[1]

	connected, _ := network.Connect(kv_host, kv_port)
	if connected == true {
	    kv_conn = server
	    fmt.Println("Reconnected to", kv_conn)
	    return true
	}
    }
    return false
}

func Request(request string) (string, bool) {
    var msg string
    var sent bool

    for {
	sent, msg = network.Send([]byte(request + "\r"))

	if sent == false {
	    reconnected := ReConnect()
	    if reconnected == false {
		fmt.Println(msg)
		log.Log(msg, "WARNING")
		return "", false
	    }
	    continue
	}
	if strings.Contains(msg, "error: leader:") {
	    leader := strings.Split(msg, ": ")[len(strings.Split(msg, ": ")) - 1]
	    leader = strings.TrimSuffix(leader, "\r\n")

	    network.Disconnect()
	    leader_ip := strings.Split(leader, ":")[0]
	    leader_port := strings.Split(leader, ":")[1]

	    connected, _ := network.Connect(leader_ip, leader_port)
	    if connected == false {
		fmt.Println("Retrying in 2 seconds...")
		time.Sleep(2 * time.Second)
		continue
	    }
	    fmt.Println("Connected to", leader)
	    continue
	}
	if msg != "server_write_lock\r\n" && msg != "error: no leader elected\r\n" {
	    break
	}
	fmt.Println("Retrying in 2 seconds...")
	time.Sleep(2 * time.Second)
    }
    return msg, true
}

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
	msg, sent := Request(strings.Join(args[1:], " "))
	if sent == true {
	    log.Log(msg, "INFO")
	    fmt.Println(msg)
	}
 
    case "push":
	if len(args) < 2 {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(helpMessage)
	    return
	}
	csvfile, err := os.Open(args[1])
	if err != nil {
	    fmt.Println(err)
	    return
	}

	reader := csv.NewReader(csvfile)
	reader.Comma = '\t'

	records, err := reader.ReadAll()
	if err != nil {
	    fmt.Println(err)
	    return
	}

	counter := 0
	for _, record := range records {
	    request := "put " + record[0] + " " + record[1]
	    Request(request)
	    counter++
	    if counter % 1000 == 0 {
		fmt.Println("Pushed", counter, "records")
	    }
	}
	csvfile.Close()

    case "pull":
	if len(args) < 3 {
	    fmt.Println("Error: Incorrect arguments")
	    fmt.Println(helpMessage)
	    return
	}
	csvfile, err := os.Open(args[1])
	if err != nil {
	    fmt.Println(err)
	    return
	}

	output, err := os.Create(args[2])
	if err != nil {
	    fmt.Println(err)
	    return
	}

	reader := csv.NewReader(csvfile)
	reader.Comma = '\t'

	records, err := reader.ReadAll()
	if err != nil {
	    fmt.Println(err)
	    return
	}

	for _, record := range records {
	    request := "get " + record[0] + " " + record[1]
	    msg, sent := Request(request)
	    if sent == false {
		fmt.Println("failed")
		log.Log("failed", "WARNING")
		break
	    }
	    result := strings.Split(msg, " ")
	    if result[0] != "get_success" {
		fmt.Println(msg)
		log.Log(msg, "WARNING")
		break
	    }
	    value := strings.Join(result[2:], " ")
	    csvline := result[1] + "\t" + value
	    csvline = strings.TrimSuffix(csvline, "\n")
	    csvline = strings.TrimSuffix(csvline, "\r")
	    csvline = strings.TrimSuffix(csvline, "\r")
	    csvline = csvline + "\n"
	    output.WriteString(csvline)
	}
	csvfile.Close()
	output.Close()

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
