package memcached

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/memcached/v4/memcachedkv"
	"go.uber.org/zap"
)

const (
	PluginName     string = "memcached"
	RootPluginName string = "kv"
)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Plugin struct {
	// config plugin
	cfgPlugin Configurer
	// logger
	log *zap.Logger
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(RootPluginName) {
		return errors.E(errors.Disabled)
	}

	p.cfgPlugin = cfg
	p.log = log.NamedLogger(PluginName)
	return nil
}

// Name returns plugin user-friendly name
func (p *Plugin) Name() string {
	return PluginName
}

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	const op = errors.Op("boltdb_plugin_provide")
	st, err := memcachedkv.NewMemcachedDriver(p.log, key, p.cfgPlugin)
	if err != nil {
		return nil, errors.E(op, err)
	}

	return st, nil
}
