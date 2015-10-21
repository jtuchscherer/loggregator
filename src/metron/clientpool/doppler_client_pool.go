package clientpool

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/loggregatorlib/loggregatorclient"
)

var ErrorEmptyClientPool = errors.New("loggregator client pool is empty")

type DopplerPool struct {
	logger *gosteno.Logger

	sync.RWMutex
	clients    map[string]loggregatorclient.Client
	clientList []loggregatorclient.Client

	servers       map[string]loggregatorclient.Client
	legacyServers map[string]loggregatorclient.Client
}

func NewDopplerPool(logger *gosteno.Logger) *DopplerPool {
	return &DopplerPool{
		logger: logger,
	}
}

func (pool *DopplerPool) Set(all map[string]loggregatorclient.Client, preferred map[string]loggregatorclient.Client) {
	pool.Lock()
	if len(preferred) > 0 {
		pool.servers = preferred
	} else if len(all) > 0 {
		pool.servers = all
	} else {
		pool.servers = nil
	}
	pool.merge()
	pool.Unlock()
}

func (pool *DopplerPool) SetLegacy(all map[string]loggregatorclient.Client, preferred map[string]loggregatorclient.Client) {
	pool.Lock()
	if len(preferred) > 0 {
		pool.legacyServers = preferred
	} else if len(all) > 0 {
		pool.legacyServers = all
	} else {
		pool.legacyServers = nil
	}
	pool.merge()
	pool.Unlock()
}

func (pool *DopplerPool) Clients() []loggregatorclient.Client {
	defer pool.RUnlock()
	pool.RLock()
	return pool.clientList
}

func (pool *DopplerPool) RandomClient() (loggregatorclient.Client, error) {
	list := pool.Clients()

	if len(list) == 0 {
		return nil, ErrorEmptyClientPool
	}

	return list[rand.Intn(len(list))], nil
}

func (pool *DopplerPool) merge() {
	newClients := map[string]loggregatorclient.Client{}

	for key, c := range pool.servers {
		newClients[key] = c
	}

	for key, c := range pool.legacyServers {
		if _, ok := newClients[key]; !ok {
			newClients[key] = c
		}
	}

	for address, client := range pool.clients {
		if _, ok := newClients[address]; !ok {
			client.Stop()
		}
	}

	newList := make([]loggregatorclient.Client, 0, len(newClients))
	for _, client := range newClients {
		newList = append(newList, client)
	}

	pool.clients = newClients
	pool.clientList = newList
}

func NewClient(logger *gosteno.Logger, url string) (loggregatorclient.Client, error) {
	if index := strings.Index(url, "://"); index > 0 {
		switch url[:index] {
		case "udp":
			return loggregatorclient.NewUDPClient(logger, url[index+3:], loggregatorclient.DefaultBufferSize)
		case "tls":
			return loggregatorclient.NewTLSClient(logger, url[index+3:])
		}
	}

	return nil, fmt.Errorf("Unknown scheme for %s", url)
}
