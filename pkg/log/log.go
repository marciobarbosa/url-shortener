// Log
//
// Provides all the necessary abstractions to handle logs.
package log

import (
    "log"
    "os"
)

const (
    ALL string	= "ALL"
    CONFIG	= "CONFIG"
    FINE	= "FINE"
    FINEST	= "FINEST"
    INFO	= "INFO"
    OFF		= "OFF"
    SEVERE	= "SEVERE"
    WARNING	= "WARNING"
)
var logLevel = INFO
var logFile *os.File = nil

// Set log level
// On success, return true and a confirmation message
// On failure, return false and the description of the problem
func SetLevel(level string) (bool, string) {
    var retMsg = ""
    var logSet = false

    switch level {
    case ALL:
    case CONFIG:
    case FINE:
    case FINEST:
    case INFO:
    case OFF:
    case SEVERE:
    case WARNING:
    default:
	retMsg = "loglevel " + level + " does not exist"
	return false, retMsg
    }
    retMsg = "loglevel set from " + logLevel + " to " + level
    logLevel = level
    logSet = true

    return logSet, retMsg
}

func Log(msg string, level string) {
    if (level == logLevel) {
	log.Printf("[%v] " + msg, logLevel)
    }
}

func Init() bool {
    flags := (os.O_APPEND | os.O_CREATE | os.O_WRONLY)

    if logFile != nil {
	logFile.Close()
    }

    logFile, err := os.OpenFile("logs.txt", flags, 0666)
    if err != nil {
	return false
    }

    log.SetOutput(logFile)
    return true
}

func InitPath(path string, name string) bool {
    flags := (os.O_APPEND | os.O_CREATE | os.O_WRONLY)

    if logFile != nil {
	logFile.Close()
    }

    logFile, err := os.OpenFile(path + "/" + name, flags, 0666)
    if err != nil {
	return false
    }

    log.SetOutput(logFile)
    return true
}

func Destroy() {
    if logFile != nil {
	logFile.Close()
	logFile = nil
    }
}
