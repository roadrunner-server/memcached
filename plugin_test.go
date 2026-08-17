package memcached

import (
	"io"
	"log/slog"
	"testing"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/memcached/v6/memcachedkv"
	"github.com/stretchr/testify/require"
)

// stubConfigurer hands the driver a pre-built config instead of decoding YAML.
type stubConfigurer struct {
	sections map[string]bool
	cfg      *memcachedkv.Config
	err      error
}

func (s *stubConfigurer) Has(name string) bool { return s.sections[name] }

func (s *stubConfigurer) UnmarshalKey(_ string, out any) error {
	if s.err != nil {
		return s.err
	}

	p, ok := out.(**memcachedkv.Config)
	if !ok {
		return errors.Str("unexpected target type")
	}
	*p = s.cfg
	return nil
}

type discardLogger struct{}

func (discardLogger) NamedLogger(string) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestInitDisabledWithoutKVSection(t *testing.T) {
	err := (&Plugin{}).Init(discardLogger{}, &stubConfigurer{sections: map[string]bool{}})

	require.Error(t, err)
	require.True(t, errors.Is(errors.Disabled, err))
}

func TestInitBuildsTracerAndLogger(t *testing.T) {
	p := &Plugin{}

	require.NoError(t, p.Init(discardLogger{}, &stubConfigurer{sections: map[string]bool{RootPluginName: true}}))

	require.NotNil(t, p.log)
	require.NotNil(t, p.tracer)
	require.NotNil(t, p.cfgPlugin)
}

func TestName(t *testing.T) {
	require.Equal(t, PluginName, (&Plugin{}).Name())
}

func TestCollectsDeclaresTracerDependency(t *testing.T) {
	require.Len(t, (&Plugin{}).Collects(), 1)
}

// TestKvFromConfigRejectsMissingSection covers the guard in the driver
// constructor: a key that decodes to no config must be reported rather than
// silently yielding a driver pointed at the default address.
func TestKvFromConfigRejectsMissingSection(t *testing.T) {
	p := &Plugin{}
	require.NoError(t, p.Init(discardLogger{}, &stubConfigurer{sections: map[string]bool{RootPluginName: true}}))

	_, err := p.KvFromConfig(t.Context(), "kv.absent")

	require.ErrorContains(t, err, "config not found by provided key")
}

func TestKvFromConfigPropagatesDecodeError(t *testing.T) {
	p := &Plugin{}
	c := &stubConfigurer{sections: map[string]bool{RootPluginName: true}}
	require.NoError(t, p.Init(discardLogger{}, c))

	c.err = errors.Str("broken config")

	_, err := p.KvFromConfig(t.Context(), "kv.broken")

	require.ErrorContains(t, err, "broken config")
}

func TestKvFromConfigBuildsDriver(t *testing.T) {
	p := &Plugin{}
	c := &stubConfigurer{
		sections: map[string]bool{RootPluginName: true},
		cfg:      &memcachedkv.Config{Addr: []string{"127.0.0.1:11211"}},
	}
	require.NoError(t, p.Init(discardLogger{}, c))

	st, err := p.KvFromConfig(t.Context(), "kv.memcached-rr")

	require.NoError(t, err)
	require.NotNil(t, st)
}

// TestConfigInitDefaults pins the fallback address used when addr is omitted.
func TestConfigInitDefaults(t *testing.T) {
	c := &memcachedkv.Config{}
	c.InitDefaults()
	require.Equal(t, []string{"127.0.0.1:11211"}, c.Addr)

	custom := &memcachedkv.Config{Addr: []string{"10.0.0.1:11211"}}
	custom.InitDefaults()
	require.Equal(t, []string{"10.0.0.1:11211"}, custom.Addr)
}
