package clientpool

import (
	"errors"
	"math/rand"
	"sync"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/loggregatorlib/loggregatorclient"
)

var ErrorEmptyClientPool = errors.New("loggregator client pool is empty")

type ServerGetters interface {
	AllServers() []string
	PreferredServers() []string
}

type DopplerClient struct {
	preferredProtocol string
	logger            *gosteno.Logger

	sync.RWMutex
	clients       map[string]loggregatorclient.LoggregatorClient
	clientList    []loggregatorclient.LoggregatorClient
	servers       []string
	legacyServers []string
}

func NewDopplerClient(logger *gosteno.Logger, preferredProtocol string) *DopplerClient {
	return &DopplerClient{
		logger:            logger,
		preferredProtocol: preferredProtocol,
	}
}

func (pool *DopplerClient) Set(all []string, preferred []string) {
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

func (pool *DopplerClient) SetLegacy(all []string, preferred []string) {
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

func (pool *DopplerClient) RandomClient() (loggregatorclient.LoggregatorClient, error) {
	list := pool.ListClients()
	if len(list) == 0 {
		return nil, ErrorEmptyClientPool
	}

	return list[rand.Intn(len(list))], nil
}

func (pool *DopplerClient) merge() {
	newClients := map[string]loggregatorclient.LoggregatorClient{}

	for _, address := range addresses {
		c := pool.clients[address]
		if c == nil {
			c = loggregatorclient.NewLoggregatorClient(address, pool.logger, loggregatorclient.DefaultBufferSize)
		}

		newClients[address] = c
	}

	for address, client := range pool.clients {
		_, ok := newClients[address]
		if !ok {
			client.Stop()
		}
	}

	pool.clients = newClients
}
