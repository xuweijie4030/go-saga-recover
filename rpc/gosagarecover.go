package rpc

import (
	"context"
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/rpcxclient"
	"github.com/xuweijie4030/go-common/gosagarecover/proto"
	"sync"
	"time"
)

type Client struct {
	XClient *rpcxclient.Client
}

var (
	c    *Client
	once sync.Once
)

func NewClient() (*Client, error) {
	var (
		err        error
		rpcXClient *rpcxclient.Client
	)

	if c == nil {
		once.Do(func() {
			rpcXClient, err = rpcxclient.NewClient(getOptions())
			if err != nil {
				return
			}
			c = &Client{
				XClient: rpcXClient,
			}
		})
	}

	return c, err
}

// 获取初始化rpcXClient客户端属性，可根据实际需求修改
func getOptions() *rpcxclient.Options {
	options := rpcxclient.DefaultOptions
	options.RegistryOption.BasePath = "/go_saga"
	options.RegistryOption.ServerName = "GoSagaRecover"
	options.RegistryOption.Addr = config.GetStringSlice("Registry.Addr")
	options.RegistryOption.Group = config.GetString("Registry.Group")
	options.Timeout = config.GetDuration("Rpc.Timeout") * time.Second

	return options
}

func (c *Client) Recover(ctx context.Context, request *proto.RecoverRequest, response *proto.RecoverResponse) (err error) {
	return c.XClient.Call(ctx, "Recover", request, response)
}


