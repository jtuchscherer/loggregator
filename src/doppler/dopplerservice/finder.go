package dopplerservice

import (
	"sync"

	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/storeadapter"
)

//go:generate counterfeiter -o fakes/fakefinder.go . Finder
type Finder interface {
	Start()
	Stop()

	// returns a set of urls (scheme://host:port)
	Addresses() []string
}

type finder struct {
	storeAdapter   storeadapter.StoreAdapter
	stopChan       chan struct{}
	storeKeyPrefix string
	onUpdate       func([]string)

	sync.RWMutex
	addressMap map[string]struct{}
	addresses  []string

	unmarshal func(value []byte) []string
	logger    *gosteno.Logger
}

func NewFinder(storeAdapter storeadapter.StoreAdapter, onUpdate func(addresses []string), logger *gosteno.Logger) Finder {
	return &finder{
		storeAdapter:   storeAdapter,
		addresses:      []string{},
		addressMap:     make(map[string]struct{}),
		stopChan:       make(chan struct{}),
		storeKeyPrefix: META_ROOT,
		unmarshal: func(value []byte) []string {
			if value != nil {
				var meta DopplerMeta
				if err := json.Unmarshal(value, &meta); err == nil {
					return meta.Endpoints
				}
			}
			return nil
		},
		onUpdate: onUpdate,
		logger:   logger,
	}
}

func NewLegacyFinder(storeAdapter storeadapter.StoreAdapter, port int, onUpdate func(addresses []string), logger *gosteno.Logger) Finder {
	return &finder{
		storeAdapter:   storeAdapter,
		addresses:      []string{},
		addressMap:     make(map[string]struct{}),
		stopChan:       make(chan struct{}),
		storeKeyPrefix: LEGACY_ROOT,
		unmarshal: func(value []byte) []string {
			if value == nil {
				return nil
			}
			return []string{fmt.Sprintf("udp://%s:%d", value, port)}
		},
		onUpdate: onUpdate,
		logger:   logger,
	}
}

func (f *finder) Start() {
	go f.run(f.stopChan)
}

func (f *finder) run(stopChan chan struct{}) {
	events, stopWatch, errors := f.storeAdapter.Watch(f.storeKeyPrefix)
	f.discoverAddresses()

	for {
		select {
		case <-stopChan:
			close(stopWatch)
			return
		case event := <-events:
			f.handleEvent(&event)
		case err := <-errors:
			f.logger.Errord(map[string]interface{}{
				"error": err,
			},
				"ServerAddressList.Run: Watch failed")
			events, stopWatch, errors = f.storeAdapter.Watch(f.storeKeyPrefix)
			f.discoverAddresses()
		}
	}
}

func (f *finder) handleEvent(event *storeadapter.WatchEvent) {
	var value []byte
	if event.Node != nil {
		value = event.Node.Value
	}
	f.Lock()
	switch event.Type {
	case storeadapter.CreateEvent:
		for _, v := range f.unmarshal(value) {
			f.addressMap[v] = struct{}{}
		}
	case storeadapter.DeleteEvent:
		fallthrough
	case storeadapter.ExpireEvent:
		prevValue := event.PrevNode.Value
		for _, v := range f.unmarshal(prevValue) {
			delete(f.addressMap, v)
		}
	case storeadapter.UpdateEvent:
		prevValue := event.PrevNode.Value
		if !bytes.Equal(value, prevValue) {
			for _, v := range f.unmarshal(prevValue) {
				delete(f.addressMap, v)
			}
			for _, v := range f.unmarshal(value) {
				f.addressMap[v] = struct{}{}
			}
		}
	}

	f.addresses = keys(f.addressMap)
	if f.onUpdate != nil {
		f.onUpdate(f.addresses)
	}
	f.Unlock()

}

func keys(serviceMap map[string]struct{}) []string {
	a := make([]string, 0, len(serviceMap))
	for k, _ := range serviceMap {
		a = append(a, k)
	}
	return a
}

func (f *finder) discoverAddresses() {
	node, err := f.storeAdapter.ListRecursively(f.storeKeyPrefix)

	if err == storeadapter.ErrorKeyNotFound {
		f.logger.Debugf("ServerAddressList.Run: Unable to recursively find keys with prefix %s", f.storeKeyPrefix)
		return
	}

	if err == storeadapter.ErrorTimeout {
		f.logger.Debug("ServerAddressList.Run: Timed out talking to store; will try again soon.")
		return
	}

	if err != nil {
		panic(err) //FIXME: understand error modes and recovery cases better
	}

	leaves := leafNodes(node)

	addressMap := make(map[string]struct{})

	for _, leaf := range leaves {
		for _, v := range f.unmarshal(leaf.Value) {
			addressMap[v] = struct{}{}
		}
	}

	addresses := keys(addressMap)

	f.Lock()
	f.addressMap = addressMap
	f.addresses = addresses

	if f.onUpdate != nil {
		f.onUpdate(f.addresses)
	}
	f.Unlock()
}

func (f *finder) Stop() {
	if f.stopChan != nil {
		close(f.stopChan)
		f.stopChan = nil
	}
}

func (f *finder) Addresses() []string {
	f.RLock()
	defer f.RUnlock()
	return f.addresses
}

func leafNodes(root storeadapter.StoreNode) []storeadapter.StoreNode {
	if !root.Dir {
		if len(root.Value) == 0 {
			return []storeadapter.StoreNode{}
		} else {
			return []storeadapter.StoreNode{root}
		}
	}

	leaves := []storeadapter.StoreNode{}
	for _, node := range root.ChildNodes {
		leaves = append(leaves, leafNodes(node)...)
	}
	return leaves
}
