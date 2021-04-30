package client

import (
	"fmt"
	"github.com/sunnyers/rpcx/log"
	"github.com/sunnyersxio/libkv"
	"github.com/sunnyersxio/libkv/store"
	"github.com/sunnyersxio/libkv/store/zmq"
	"strings"
	"sync"
	"time"
)

func init() {
	zmq.Register()
}

// RedisDiscovery is a redis service discovery.
// It always returns the registered servers in redis.
type ZmqDiscovery struct {
	basePath string
	kv       store.Store
	pairsMu  sync.RWMutex
	pairs    []*KVPair
	chans    []chan []*KVPair
	mu       sync.Mutex
	
	// -1 means it always retry to watch until zookeeper is ok, 0 means no retry.
	RetriesAfterWatchFailed int
	
	filter ServiceDiscoveryFilter
	
	stopCh chan struct{}
}

// NewRedisDiscovery returns a new RedisDiscovery.
func NewZmqDiscovery(basePath string, servicePath string, etcdAddr []string, options *store.Config) (ServiceDiscovery, error) {
	kv, err := libkv.NewStore(store.ZMQ, etcdAddr, options)
	if err != nil {
		log.Infof("cannot create store: %v", err)
		return nil, err
	}
	
	return NewZmqDiscoveryStore(basePath, kv)
}

// NewRedisDiscoveryStore return a new RedisDiscovery with specified store.
func NewZmqDiscoveryStore(basePath string, kv store.Store) (ServiceDiscovery, error) {
	d := &ZmqDiscovery{basePath: basePath, kv: kv}
	d.stopCh = make(chan struct{})
	
	ps, err := kv.Get(basePath)
	if err == nil {
		log.Infof("cannot get services of from registry: %v, err: %v", basePath, err)
		panic(err)
	}
	
	fmt.Println(ps)
	//
	//pairs := make([]*KVPair, 0, len(ps))
	//var prefix string
	//for _, p := range ps {
	//	if prefix == "" {
	//		if strings.HasPrefix(p.Key, "/") {
	//			if strings.HasPrefix(d.basePath, "/") {
	//				prefix = d.basePath + "/"
	//			} else {
	//				prefix = "/" + d.basePath + "/"
	//			}
	//		} else {
	//			if strings.HasPrefix(d.basePath, "/") {
	//				prefix = d.basePath[1:] + "/"
	//			} else {
	//				prefix = d.basePath + "/"
	//			}
	//		}
	//	}
	//	if p.Key == prefix[:len(prefix)-1] {
	//		continue
	//	}
	//	k := strings.TrimPrefix(p.Key, prefix)
	//	pair := &KVPair{Key: k, Value: string(p.Value)}
	//	if d.filter != nil && !d.filter(pair) {
	//		continue
	//	}
	//	pairs = append(pairs, pair)
	//}
	d.pairsMu.Lock()
	//d.pairs = pairs
	d.pairsMu.Unlock()
	d.RetriesAfterWatchFailed = -1
	
	go d.watch()
	return d, nil
}

// NewRedisDiscoveryTemplate returns a new RedisDiscovery template.
func NewZmqDiscoveryTemplate(basePath string, etcdAddr []string, options *store.Config) (ServiceDiscovery, error) {
	kv, err := libkv.NewStore(store.ZMQ, etcdAddr, options)
	if err != nil {
		log.Infof("cannot create store: %v", err)
		return nil, err
	}
	
	return NewZmqDiscoveryStore(basePath, kv)
}

// Clone clones this ServiceDiscovery with new servicePath.
func (d *ZmqDiscovery) Clone(servicePath string) (ServiceDiscovery, error) {
	return NewZmqDiscoveryStore(d.basePath, d.kv)
}

// SetFilter sets the filer.
func (d *ZmqDiscovery) SetFilter(filter ServiceDiscoveryFilter) {
	d.filter = filter
}

// GetServices returns the servers
func (d *ZmqDiscovery) GetServices() []*KVPair {
	d.pairsMu.RLock()
	defer d.pairsMu.RUnlock()
	
	return d.pairs
}

// WatchService returns a nil chan.
func (d *ZmqDiscovery) WatchService() chan []*KVPair {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	ch := make(chan []*KVPair, 10)
	d.chans = append(d.chans, ch)
	return ch
}

func (d *ZmqDiscovery) RemoveWatcher(ch chan []*KVPair) {
	d.mu.Lock()
	defer d.mu.Unlock()
	
	var chans []chan []*KVPair
	for _, c := range d.chans {
		if c == ch {
			continue
		}
		
		chans = append(chans, c)
	}
	
	d.chans = chans
}

func (d *ZmqDiscovery) watch() {
	defer func() {
		d.kv.Close()
	}()
	
	for {
		var err error
		var c <-chan []*store.KVPair
		var tempDelay time.Duration
		
		retry := d.RetriesAfterWatchFailed
		for d.RetriesAfterWatchFailed < 0 || retry >= 0 {
			c, err = d.kv.WatchTree(d.basePath, nil)
			if err != nil {
				if d.RetriesAfterWatchFailed > 0 {
					retry--
				}
				if tempDelay == 0 {
					tempDelay = 1 * time.Second
				} else {
					tempDelay *= 2
				}
				if max := 30 * time.Second; tempDelay > max {
					tempDelay = max
				}
				log.Warnf("can not watchtree (with retry %d, sleep %v): %s: %v", retry, tempDelay, d.basePath, err)
				time.Sleep(tempDelay)
				continue
			}
			break
		}
		
		if err != nil {
			log.Errorf("can't watch %s: %v", d.basePath, err)
			return
		}
	
	readChanges:
		for {
			select {
			case <-d.stopCh:
				log.Info("discovery has been closed")
				return
			case ps, ok := <-c:
				if !ok {
					break readChanges
				}
				var pairs []*KVPair // latest servers
				if ps == nil {
					d.pairsMu.Lock()
					d.pairs = pairs
					d.pairsMu.Unlock()
					continue
				}
				
				var prefix string
				for _, p := range ps {
					if prefix == "" {
						if strings.HasPrefix(p.Key, "/") {
							if strings.HasPrefix(d.basePath, "/") {
								prefix = d.basePath + "/"
							} else {
								prefix = "/" + d.basePath + "/"
							}
						} else {
							if strings.HasPrefix(d.basePath, "/") {
								prefix = d.basePath[1:] + "/"
							} else {
								prefix = d.basePath + "/"
							}
						}
					}
					if p.Key == prefix[:len(prefix)-1] {
						continue
					}
					
					k := strings.TrimPrefix(p.Key, prefix)
					pair := &KVPair{Key: k, Value: string(p.Value)}
					if d.filter != nil && !d.filter(pair) {
						continue
					}
					pairs = append(pairs, pair)
				}
				d.pairsMu.Lock()
				d.pairs = pairs
				d.pairsMu.Unlock()
				
				d.mu.Lock()
				for _, ch := range d.chans {
					ch := ch
					go func() {
						defer func() {
							recover()
						}()
						
						select {
						case ch <- pairs:
						case <-time.After(time.Minute):
							log.Warn("chan is full and new change has been dropped")
						}
					}()
				}
				d.mu.Unlock()
			}
		}
		
		log.Warn("chan is closed and will rewatch")
	}
}

func (d *ZmqDiscovery) Close() {
	close(d.stopCh)
}
