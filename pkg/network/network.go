// Network
//
// Provides all the necessary abstractions to handle network communication.
package network

import (
    "net"
)

var conn *net.TCPConn = nil

// Connect to the host:port received as arugment
// On success, return true and the received reply
// On failure, return false and the description of the problem
func Connect(host string, port string) (bool, string) {
    addr, err := net.ResolveTCPAddr("tcp", host + ":" + port)
    if err != nil {
	return false, "Could not resolve " + host + ":" + port
    }

    if conn != nil {
	conn.Close()
    }

    conn, err = net.DialTCP("tcp", nil, addr)
    if err != nil {
	return false, "Could not connect to " + host + ":" + port
    }

    buffer := make([]byte, 128 * 1024)
    nbytes, err := conn.Read(buffer)
    if err != nil {
	return false, "Could not receive reply from the server"
    }

    return true, string(buffer[:nbytes])
}

// Send data to the connected host
// On success, return true and the received reply
// On failure, return false and the description of the problem
func Send(data []byte) (bool, string) {
    if conn == nil {
	return false, "Error! Not connected!"
    }

    _, err := conn.Write(data)
    if err != nil {
	return false, "Could not send message to the server"
    }

    buffer := make([]byte, 128 * 1024)
    nbytes, err := conn.Read(buffer)
    if err != nil {
	return false, "Could not receive reply from the server"
    }

    return true, string(buffer[:nbytes])
}

// Destroy existing connection
func Disconnect() {
    if conn != nil {
	conn.Close()
	conn = nil
    }
}
