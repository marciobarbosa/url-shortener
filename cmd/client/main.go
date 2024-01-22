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
		fmt.Println(msg)
		log.Log(msg, "WARNING")
		return
	    }
	    if msg != "server_write_lock\r\n" {
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
