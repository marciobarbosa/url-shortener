package conf

import (
    "io/ioutil"
    "encoding/json"
)

type Server struct {
    IP   string `json:"ip"`
    Port string `json:"port"`
}

type ServerConfig struct {
    Servers []Server `json:"servers"`
}

func GetServers(confpath string) ([]string, error) {
    var servers []string
    var serverConfig ServerConfig

    jsonconf, err := ioutil.ReadFile(confpath)
    if err != nil {
	return nil, err
    }

    err = json.Unmarshal(jsonconf, &serverConfig)
    if err != nil {
	return nil, err
    }

    for _, server := range serverConfig.Servers {
	addr := server.IP + ":" + server.Port
	servers = append(servers, addr)
    }
    return servers, nil
}
