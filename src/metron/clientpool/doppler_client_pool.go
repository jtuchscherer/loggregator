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

type DopplerClient struct {
	preferredProtocol string
	logger            *gosteno.Logger

	sync.RWMutex
	clients    map[string]loggregatorclient.Client
	clientList []loggregatorclient.Client

	servers       map[string]string
	legacyServers map[string]string
}

func NewDopplerClient(logger *gosteno.Logger, preferredProtocol string) *DopplerClient {
	return &DopplerClient{
		logger:            logger,
		preferredProtocol: preferredProtocol,
	}
}

func (pool *DopplerClient) Set(all map[string]string, preferred map[string]string) {
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

func (pool *DopplerClient) SetLegacy(all map[string]string, preferred map[string]string) {
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

func (pool *DopplerClient) Clients() []loggregatorclient.Client {
	defer pool.RUnlock()
	pool.RLock()
	return pool.clientList
}

func (pool *DopplerClient) RandomClient() (loggregatorclient.Client, error) {
	list := pool.Clients()

	if len(list) == 0 {
		return nil, ErrorEmptyClientPool
	}

	return list[rand.Intn(len(list))], nil
}

func (pool *DopplerClient) merge() {
	newClients := map[string]loggregatorclient.Client{}

	for key, url := range pool.servers {
		c, err := pool.newClient(url)
		if err != nil {
			pool.logger.Errord(map[string]interface{}{
				"key": key, "err": err,
			}, "Invalid url")

			continue
		}

		newClients[key] = c
	}

	for key, url := range pool.legacyServers {
		if _, ok := newClients[key]; !ok {
			c, err := pool.newClient(url)
			if err != nil {
				pool.logger.Errord(map[string]interface{}{
					"key": key, "err": err,
				}, "Invalid url")

				continue
			}

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

func (pool *DopplerClient) newClient(url string) (loggregatorclient.Client, error) {
	if index := strings.Index(url, "://"); index > 0 {
		switch url[:index] {
		case "udp":
			return loggregatorclient.NewUDPClient(pool.logger, url[index+3:], loggregatorclient.DefaultBufferSize), nil
		case "tls":
			return nil, errors.New("tls not implement")
		}
	}

	return nil, fmt.Errorf("Unknown scheme for %s", url)
}
