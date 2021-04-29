package serverplugin

import (
	"context"
	"errors"
	"fmt"
	"github.com/rcrowley/go-metrics"
	"github.com/sunnyers/rpcx/log"
	"github.com/sunnyersxio/libkv"
	"github.com/sunnyersxio/libkv/store"
	"github.com/sunnyersxio/libkv/store/zmq"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

func init() {
	zmq.Register()
}

// ZooKeeperRegisterPlugin implements zookeeper registry.
type ZmqRegisterPlugin struct {
	// service address, for example, tcp@127.0.0.1:8972, quic@127.0.0.1:1234
	ServiceAddress string
	// zookeeper addresses
	zmqServers []string
	// base path for rpcx server, for example com/example/rpcx
	BasePath string
	Metrics  metrics.Registry
	// Registered services
	Services       []string
	metasLock      sync.RWMutex
	metas          map[string]string
	UpdateInterval time.Duration
	
	Options *store.Config
	kv      store.Store
	
	dying chan struct{}
	done  chan struct{}
}

// Start starts to connect zookeeper cluster
func (p *ZmqRegisterPlugin) Start() error {
	if p.done == nil {
		p.done = make(chan struct{})
	}
	if p.dying == nil {
		p.dying = make(chan struct{})
	}
	
	if p.kv == nil {
		kv, err := libkv.NewStore(store.ZMQ, p.zmqServers, p.Options)
		if err != nil {
			log.Fatalf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}
	err := p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Fatalf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}
	
	if p.UpdateInterval > 0 {
		ticker := time.NewTicker(p.UpdateInterval)
		go func() {
			defer p.kv.Close()
			
			// refresh service TTL
			for {
				select {
				case <-p.dying:
					close(p.done)
					return
				case <-ticker.C:
					extra := make(map[string]string)
					if p.Metrics != nil {
						extra["calls"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("calls", p.Metrics).RateMean())
						extra["connections"] = fmt.Sprintf("%.2f", metrics.GetOrRegisterMeter("connections", p.Metrics).RateMean())
					}
					//set this same metrics for all services at this server
					for _, name := range p.Services {
						kvPaire, err := p.kv.Get(p.BasePath)
						if err != nil {
							log.Infof("can't get data of node: %s, because of %v", p.BasePath, err.Error())
							
							p.metasLock.RLock()
							meta := p.metas[name]
							p.metasLock.RUnlock()
							
							err = p.kv.Put(p.BasePath, []byte(meta), &store.WriteOptions{TTL: p.UpdateInterval * 2})
							if err != nil {
								log.Errorf("cannot re-create zookeeper path %s: %v", p.BasePath, err)
							}
						} else {
							v, _ := url.ParseQuery(string(kvPaire.Value))
							for key, value := range extra {
								v.Set(key, value)
							}
							p.kv.Put(p.BasePath, []byte(v.Encode()), &store.WriteOptions{TTL: p.UpdateInterval * 2})
						}
					}
				}
			}
		}()
	}
	
	return nil
}

// Stop unregister all services.
func (p *ZmqRegisterPlugin) Stop() error {
	if p.kv == nil {
		kv, err := libkv.NewStore(store.ZK, p.zmqServers, p.Options)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}
	
	
	for _, name := range p.Services {
		exist, err := p.kv.Exists(p.BasePath)
		if err != nil {
			log.Errorf("cannot delete zk path %s: %v", p.BasePath, err)
			continue
		}
		if exist {
			p.kv.Delete(p.BasePath)
			log.Infof("delete zk path %s", p.BasePath,name, err)
		}
	}
	
	close(p.dying)
	<-p.done
	
	return nil
}

// HandleConnAccept handles connections from clients
func (p *ZmqRegisterPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("connections", p.Metrics).Mark(1)
	}
	return conn, true
}

// PreCall handles rpc call from clients
func (p *ZmqRegisterPlugin) PreCall(_ context.Context, _, _ string, args interface{}) (interface{}, error) {
	if p.Metrics != nil {
		metrics.GetOrRegisterMeter("calls", p.Metrics).Mark(1)
	}
	return args, nil
}

// Register handles registering event.
// this service is registered at BASE/serviceName/thisIpAddress node
func (p *ZmqRegisterPlugin) Register(name string, rcvr interface{}, metadata string) (err error) {
	if strings.TrimSpace(name) == "" {
		err = errors.New("Register service `name` can't be empty")
		return
	}
	
	if p.kv == nil {
		zmq.Register()
		kv, err := libkv.NewStore(store.ZMQ, p.zmqServers, nil)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}
	
	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}
	
	p.Services = append(p.Services, name)
	
	p.metasLock.Lock()
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	p.metas[name] = metadata
	p.metasLock.Unlock()
	return
}

func (p *ZmqRegisterPlugin) RegisterFunction(serviceName, fname string, fn interface{}, metadata string) error {
	return p.Register(serviceName, fn, metadata)
}

func (p *ZmqRegisterPlugin) Unregister(name string) (err error) {
	if len(p.Services) == 0 {
		return nil
	}
	
	if strings.TrimSpace(name) == "" {
		return errors.New("Register service `name` can't be empty")
	}
	
	if p.kv == nil {
		zmq.Register()
		kv, err := libkv.NewStore(store.ZMQ, p.zmqServers, nil)
		if err != nil {
			log.Errorf("cannot create zk registry: %v", err)
			return err
		}
		p.kv = kv
	}
	
	err = p.kv.Put(p.BasePath, []byte("rpcx_path"), &store.WriteOptions{IsDir: true})
	if err != nil {
		log.Errorf("cannot create zk path %s: %v", p.BasePath, err)
		return err
	}
	
	var services = make([]string, 0, len(p.Services)-1)
	for _, s := range p.Services {
		if s != name {
			services = append(services, s)
		}
	}
	p.Services = services
	
	p.metasLock.Lock()
	if p.metas == nil {
		p.metas = make(map[string]string)
	}
	delete(p.metas, name)
	p.metasLock.Unlock()
	
	return nil
}
