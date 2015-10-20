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
	clients    map[string]loggregatorclient.LoggregatorClient
	clientList []loggregatorclient.LoggregatorClient
	logger     *gosteno.Logger
	sync.RWMutex
	allServers    ServerGetters
	legacyServers ServerGetters

	preferredProtocol string

	index    int
	previous []string
}

func NewDopplerClient(logger *gosteno.Logger, preferredProtocol string, servers ServerGetters, legacyServers ServerGetters) *DopplerClient {
	return &DopplerClient{
		logger:            logger,
		allServers:        servers,
		legacyServers:     legacyServers,
		preferredProtocol: preferredProtocol,
	}
}

func (pool *DopplerClient) ListClients() []loggregatorclient.LoggregatorClient {
	all := pool.allServers.Addresses()
	pool.syncWithAddressList(pool.inZone(all))

	if len(pool.clients) == 0 {
		pool.syncWithAddressList(all)
	}

	all = pool.legacyServers.Addresses()
	if len(pool.clients) == 0 {
		pool.syncWithAddressList(pool.inZone(all))
	}

	if len(pool.clients) == 0 {
		pool.syncWithAddressList(all)
	}

	pool.RLock()
	defer pool.RUnlock()

	val := make([]loggregatorclient.LoggregatorClient, 0, len(pool.clients))
	for _, client := range pool.clients {
		val = append(val, client)
	}

	return val
}

func (pool *DopplerClient) RandomClient() (loggregatorclient.LoggregatorClient, error) {
	list := pool.ListClients()
	if len(list) == 0 {
		return nil, ErrorEmptyClientPool
	}

	return list[rand.Intn(len(list))], nil
}

func (pool *DopplerClient) syncWithAddressList(addresses []string) {
	newClients := map[string]loggregatorclient.LoggregatorClient{}

	pool.Lock()
	defer pool.Unlock()

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
