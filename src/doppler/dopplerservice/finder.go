package dopplerservice

import (
	"strings"
	"sync"

	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/storeadapter"
)

//go:generate counterfeiter -o fakes/fakefinder.go . Finder
type Finder interface {
	Start()
	Stop()

	// returns a map of doppler id to url (scheme://host:port)
	AllServers() map[string]string
	PreferredServers() map[string]string
}

type finder struct {
	storeAdapter   storeadapter.StoreAdapter
	protocolPrefix string
	stopChan       chan struct{}
	storeKeyPrefix string
	onUpdate       func(all map[string]string, preferred map[string]string)
	preferred      func(relativeKey string) bool

	sync.RWMutex
	addressMap   map[string]string
	preferredMap map[string]string

	unmarshal func(value []byte) []string
	logger    *gosteno.Logger
}

func NewFinder(storeAdapter storeadapter.StoreAdapter, preferredProtocol string, preferred func(key string) bool, onUpdate func(all map[string]string, preferred map[string]string), logger *gosteno.Logger) (Finder, error) {
	if preferredProtocol != "tls" && preferredProtocol != "udp" {
		return nil, errors.New("Invalid protocol")
	}

	return &finder{
		storeAdapter:   storeAdapter,
		protocolPrefix: preferredProtocol + "://",
		addressMap:     map[string]string{},
		preferredMap:   map[string]string{},

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
		onUpdate:  onUpdate,
		preferred: preferred,
		logger:    logger,
	}, nil
}

func NewLegacyFinder(storeAdapter storeadapter.StoreAdapter, port int, preferred func(key string) bool, onUpdate func(all map[string]string, preferred map[string]string), logger *gosteno.Logger) Finder {
	return &finder{
		storeAdapter:   storeAdapter,
		protocolPrefix: "udp://",
		addressMap:     map[string]string{},
		preferredMap:   map[string]string{},

		stopChan:       make(chan struct{}),
		storeKeyPrefix: LEGACY_ROOT,
		unmarshal: func(value []byte) []string {
			if value == nil {
				return nil
			}
			return []string{fmt.Sprintf("udp://%s:%d", value, port)}
		},
		onUpdate:  onUpdate,
		preferred: preferred,
		logger:    logger,
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
		if len(event.Node.Key) <= len(f.storeKeyPrefix) {
			return
		}
		value = event.Node.Value
	}
	if event.PrevNode != nil {
		if len(event.PrevNode.Key) <= len(f.storeKeyPrefix) {
			return
		}
	}

	f.Lock()
	switch event.Type {
	case storeadapter.CreateEvent:
		rPath := event.Node.Key[len(f.storeKeyPrefix):]
		preferred := f.preferred(rPath)
		url, ok := f.preferredUrl(f.unmarshal(value))
		if !ok {
			f.logger.Errord(map[string]interface{}{
				"key":   event.Node.Key,
				"value": event.Node.Value,
			}, "Invalid data")
			return
		}

		f.addressMap[rPath] = url
		if preferred {
			f.preferredMap[rPath] = url
		}
	case storeadapter.DeleteEvent:
		fallthrough
	case storeadapter.ExpireEvent:
		rPath := event.PrevNode.Key[len(f.storeKeyPrefix):]
		delete(f.addressMap, rPath)
		delete(f.preferredMap, rPath)
	case storeadapter.UpdateEvent:
		rPath := event.PrevNode.Key[len(f.storeKeyPrefix):]
		prevValue := event.PrevNode.Value
		preferred := f.preferred(rPath)
		if !bytes.Equal(value, prevValue) {
			url, ok := f.preferredUrl(f.unmarshal(value))
			if !ok {
				f.logger.Errord(map[string]interface{}{
					"key":   event.Node.Key,
					"value": event.Node.Value,
				}, "Invalid data")
				return
			}

			f.addressMap[rPath] = url
			if preferred {
				f.preferredMap[rPath] = url
			}
		}
	}

	if f.onUpdate != nil {
		f.onUpdate(f.addressMap, f.preferredMap)
	}
	f.Unlock()

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

	addressMap := map[string]string{}
	preferredMap := map[string]string{}

	for _, leaf := range leaves {
		rPath := leaf.Key[len(f.storeKeyPrefix):]
		preferred := f.preferred(rPath)

		url, ok := f.preferredUrl(f.unmarshal(leaf.Value))
		if !ok {
			f.logger.Errord(map[string]interface{}{
				"key":   leaf.Key,
				"value": leaf.Value,
			}, "Invalid data")
			return
		}

		addressMap[rPath] = url
		if preferred {
			preferredMap[rPath] = url
		}
	}

	f.Lock()
	f.addressMap = addressMap
	f.preferredMap = preferredMap

	if f.onUpdate != nil {
		f.onUpdate(f.addressMap, f.preferredMap)
	}
	f.Unlock()
}

func (f *finder) preferredUrl(urls []string) (string, bool) {
	switch len(urls) {
	case 0:
		//
	case 1:
		if strings.HasPrefix(urls[0], "udp://") {
			return urls[0], true
		}
	default:
		u, ok := findWithProtocol(f.protocolPrefix, urls)
		if !ok {
			u, ok = findWithProtocol("udp://", urls)
		}
		return u, ok
	}

	return "", false
}

func findWithProtocol(protocol string, urls []string) (string, bool) {
	for _, u := range urls {
		if strings.HasPrefix(u, protocol) {
			return u, true
		}
	}
	return "", false
}

func (f *finder) Stop() {
	if f.stopChan != nil {
		close(f.stopChan)
		f.stopChan = nil
	}
}

func (f *finder) AllServers() map[string]string {
	result := map[string]string{}
	f.RLock()
	defer f.RUnlock()
	for k, v := range f.addressMap {
		result[k] = v
	}
	return result
}

func (f *finder) PreferredServers() map[string]string {
	result := map[string]string{}
	f.RLock()
	defer f.RUnlock()
	for k, v := range f.preferredMap {
		result[k] = v
	}
	return result
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
