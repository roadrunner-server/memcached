package kv

import (
	"net/rpc"
	"testing"

	"tests/helpers"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v1"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/memcached/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/require"
)

const (
	rpcAddr = "127.0.0.1:6001"
	storage = "memcached-rr"
)

func memcachedPlugins() []any {
	return []any{&kv.Plugin{}, &memcached.Plugin{}, &rpcPlugin.Plugin{}}
}

// bootKV starts the container and hands back a connected rpc client with the
// storage emptied, so each test begins from a known state. The memcached
// container outlives individual tests, unlike the RoadRunner container.
func bootKV(t *testing.T) *rpc.Client {
	t.Helper()

	helpers.Start(t, "configs/.rr-memcached.yaml", memcachedPlugins(), helpers.WithTCPProbe(rpcAddr))

	client := helpers.NewRPCClient(t, rpcAddr)
	require.NoError(t, client.Call("kv.Clear", &kvProto.Request{Storage: storage}, &kvProto.Response{}))

	return client
}

// items builds a request carrying the given key/value pairs. A blank value
// means the item is a key-only reference, which is what Has, MGet and Delete
// take.
func items(pairs map[string]string) *kvProto.Request {
	req := &kvProto.Request{Storage: storage}
	for k, v := range pairs {
		item := &kvProto.Item{Key: k}
		if v != "" {
			item.Value = []byte(v)
		}
		req.Items = append(req.Items, item)
	}
	return req
}

func keys(names ...string) *kvProto.Request {
	req := &kvProto.Request{Storage: storage}
	for _, n := range names {
		req.Items = append(req.Items, &kvProto.Item{Key: n})
	}
	return req
}

// has returns how many of the given keys the storage currently holds.
func has(t *testing.T, client *rpc.Client, names ...string) int {
	t.Helper()

	resp := &kvProto.Response{}
	require.NoError(t, client.Call("kv.Has", keys(names...), resp))

	return len(resp.GetItems())
}

func TestSetAndHas(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))

	require.Equal(t, 2, has(t, client, "a", "b"))
	require.Equal(t, 0, has(t, client, "missing"))
}

// TestMGetReturnsStoredValues checks MGet hands back the bytes that were set,
// not merely the keys.
func TestMGetReturnsStoredValues(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))

	resp := &kvProto.Response{}
	require.NoError(t, client.Call("kv.MGet", keys("a", "b", "absent"), resp))

	got := make(map[string]string, len(resp.GetItems()))
	for _, it := range resp.GetItems() {
		got[it.GetKey()] = string(it.GetValue())
	}

	require.Equal(t, map[string]string{"a": "aa", "b": "bb"}, got)
}

func TestDeleteRemovesOnlyTheNamedKey(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))
	require.NoError(t, client.Call("kv.Delete", keys("a"), &kvProto.Response{}))

	require.Equal(t, 0, has(t, client, "a"))
	require.Equal(t, 1, has(t, client, "b"))
}

func TestClearEmptiesTheStorage(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", items(map[string]string{"a": "aa", "b": "bb", "c": "cc"}), &kvProto.Response{}))
	require.Equal(t, 3, has(t, client, "a", "b", "c"))

	require.NoError(t, client.Call("kv.Clear", &kvProto.Request{Storage: storage}, &kvProto.Response{}))

	require.Equal(t, 0, has(t, client, "a", "b", "c"))
}

// TestUnknownStorageIsRejected covers the driver lookup: a storage name that is
// not configured must produce an error rather than a silent no-op.
func TestUnknownStorageIsRejected(t *testing.T) {
	client := bootKV(t)

	err := client.Call("kv.Has", &kvProto.Request{
		Storage: "not-configured",
		Items:   []*kvProto.Item{{Key: "a"}},
	}, &kvProto.Response{})

	require.Error(t, err)
}
