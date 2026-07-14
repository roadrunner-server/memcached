package kv

import (
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memcached/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newRPCClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	var conn net.Conn
	var err error
	d := &net.Dialer{}
	for range 10 {
		conn, err = d.DialContext(t.Context(), "tcp", address)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
	assert.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestMemcached(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-memcached.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&memcached.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 1)
	t.Run("MEMCACHED", testRPCMethodsMemcached)
	stopCh <- struct{}{}
	wg.Wait()
}

func testRPCMethodsMemcached(t *testing.T) {
	const storage = "memcached-rr"

	client := newRPCClient(t, "127.0.0.1:6001")

	tt := durationpb.New(time.Second * 5)

	keys := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "c"},
		},
	}

	data := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb")},
			{Key: "c", Value: []byte("cc"), Ttl: tt},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	err := client.Call("kv.Set", data, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp := &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 3)

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.MGet", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2) // c is expired

	tt2 := durationpb.New(time.Second * 10)

	data2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Ttl: tt2},
			{Key: "b", Ttl: tt2},
			{Key: "d", Ttl: tt2},
		},
	}

	err = client.Call("kv.MExpire", data2, &kvProto.KvResponse{})
	assert.NoError(t, err)

	keys2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "d"},
		},
	}

	// TTL is not supported by the memcached driver
	err = client.Call("kv.TTL", keys2, &kvProto.KvResponse{})
	assert.Error(t, err)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys2, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())

	keysDel := &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "e"}},
	}

	err = client.Call("kv.Delete", keysDel, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keysDel, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())

	dataClear := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb")},
			{Key: "c", Value: []byte("cc")},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	err = client.Call("kv.Set", dataClear, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 5)

	err = client.Call("kv.Clear", &kvProto.KvRequest{Storage: storage}, &kvProto.KvResponse{})
	assert.NoError(t, err)

	time.Sleep(time.Second * 2)
	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())
}
