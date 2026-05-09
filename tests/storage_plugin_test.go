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
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
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
	}()

	time.Sleep(time.Second * 1)
	t.Run("MEMCACHED", testRPCMethodsMemcached)
	stopCh <- struct{}{}
	wg.Wait()
}

func testRPCMethodsMemcached(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	// add 5 second ttl
	tt := durationpb.New(time.Second * 5)

	keys := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key: "a",
			},
			{
				Key: "b",
			},
			{
				Key: "c",
			},
		},
	}

	data := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key:   "a",
				Value: []byte("aa"),
			},
			{
				Key:   "b",
				Value: []byte("bb"),
			},
			{
				Key:     "c",
				Value:   []byte("cc"),
				Ttl: tt,
			},
			{
				Key:   "d",
				Value: []byte("dd"),
			},
			{
				Key:   "e",
				Value: []byte("ee"),
			},
		},
	}

	ret := &kvProto.KvResponse{}
	// Register 3 keys with values
	err = client.Call("kv.Set", data, ret)
	assert.NoError(t, err)

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 3) // should be 3

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2) // should be 2

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.MGet", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2) // c is expired

	tt2 := durationpb.New(time.Second * 10)

	data2 := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key:     "a",
				Ttl: tt2,
			},
			{
				Key:     "b",
				Ttl: tt2,
			},
			{
				Key:     "d",
				Ttl: tt2,
			},
		},
	}

	// MEXPIRE
	ret = &kvProto.KvResponse{}
	err = client.Call("kv.MExpire", data2, ret)
	assert.NoError(t, err)

	// TTL call is not supported for the memcached driver
	keys2 := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key: "a",
			},
			{
				Key: "b",
			},
			{
				Key: "d",
			},
		},
	}

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.TTL", keys2, ret)
	assert.Error(t, err)
	assert.Len(t, ret.GetItems(), 0)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys2, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0)

	// DELETE
	keysDel := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key: "e",
			},
		},
	}

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Delete", keysDel, ret)
	assert.NoError(t, err)

	// HAS AFTER DELETE
	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keysDel, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0)

	dataClear := &kvProto.KvRequest{
		Storage: "memcached-rr",
		Items: []*kvProto.KvItem{
			{
				Key:   "a",
				Value: []byte("aa"),
			},
			{
				Key:   "b",
				Value: []byte("bb"),
			},
			{
				Key:   "c",
				Value: []byte("cc"),
			},
			{
				Key:   "d",
				Value: []byte("dd"),
			},
			{
				Key:   "e",
				Value: []byte("ee"),
			},
		},
	}

	clr := &kvProto.KvRequest{Storage: "memcached-rr"}

	ret = &kvProto.KvResponse{}
	// Register 3 keys with values
	err = client.Call("kv.Set", dataClear, ret)
	assert.NoError(t, err)

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 5) // should be 5

	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Clear", clr, ret)
	assert.NoError(t, err)

	time.Sleep(time.Second * 2)
	ret = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0) // should be 5
}
