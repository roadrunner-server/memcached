package kv

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/api-go/v6/kv/v2/kvV2connect"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memcached/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/durationpb"
)

func newKVClient(t *testing.T, address string) kvV2connect.KvServiceClient {
	t.Helper()
	httpc := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return new(net.Dialer).DialContext(ctx, network, addr)
		},
	}}
	t.Cleanup(httpc.CloseIdleConnections)
	return kvV2connect.NewKvServiceClient(httpc, "http://"+address)
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

	client := newKVClient(t, "127.0.0.1:6001")
	ctx := t.Context()

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

	_, err := client.Set(ctx, connect.NewRequest(data))
	assert.NoError(t, err)

	resp, err := client.Has(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 3)

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	resp, err = client.Has(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 2)

	resp, err = client.MGet(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 2) // c is expired

	tt2 := durationpb.New(time.Second * 10)

	data2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Ttl: tt2},
			{Key: "b", Ttl: tt2},
			{Key: "d", Ttl: tt2},
		},
	}

	_, err = client.MExpire(ctx, connect.NewRequest(data2))
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
	_, err = client.TTL(ctx, connect.NewRequest(keys2))
	assert.Error(t, err)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	resp, err = client.Has(ctx, connect.NewRequest(keys2))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())

	keysDel := &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "e"}},
	}

	_, err = client.Delete(ctx, connect.NewRequest(keysDel))
	assert.NoError(t, err)

	resp, err = client.Has(ctx, connect.NewRequest(keysDel))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())

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

	_, err = client.Set(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)

	resp, err = client.Has(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 5)

	_, err = client.Clear(ctx, connect.NewRequest(&kvProto.KvRequest{Storage: storage}))
	assert.NoError(t, err)

	time.Sleep(time.Second * 2)
	resp, err = client.Has(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())
}
