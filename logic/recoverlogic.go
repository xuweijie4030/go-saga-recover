package logic

import (
	"context"
	"encoding/json"
	"github.com/carefreex-io/config"
	"github.com/carefreex-io/logger"
	etcdClient "github.com/rpcxio/rpcx-etcd/client"
	"github.com/smallnest/rpcx/client"
	"github.com/xuweijie4030/go-common/gosaga/proto"
	"go-saga-recover/common"
	"go-saga-recover/db"
	"sync"
)

type RecoverLogic struct {
	signal chan context.Context
}

var (
	recoverLogic     *RecoverLogic
	recoverLogicOnce sync.Once
)

func NewRecoverLogic() *RecoverLogic {
	if recoverLogic != nil {
		return recoverLogic
	}
	recoverLogicOnce.Do(func() {
		recoverLogic = &RecoverLogic{
			signal: make(chan context.Context),
		}
		go recoverLogic.schedule()
	})

	return recoverLogic
}

func (l *RecoverLogic) schedule() {
	for ctx := range l.signal {
		NewRecoverLogic().Handle(ctx)
	}
}

func (l *RecoverLogic) Add(ctx context.Context) {
	l.signal <- ctx
}

func (l *RecoverLogic) Handle(ctx context.Context) {
	sagaIds, err := db.NewSagaDb(ctx).GetWaitRecoverSagaId()
	if err != nil {
		logger.ErrorfX(ctx, "get wait recover saga id failed: err=%v", err)
		return
	}
	if len(sagaIds) == 0 {
		logger.InfofX(ctx, "not found any wait recover saga")
		return
	}
	servers, err := l.getSagaServices(ctx)
	if err != nil {
		logger.ErrorfX(ctx, "get saga servers failed: err=%v", err)
		return
	}
	if len(servers) == 0 {
		logger.InfofX(ctx, "not found any saga server")
		return
	}
	logger.InfofX(ctx, "servers=%v", servers)

	clients := l.getClients(ctx, servers)

	sagaIds = l.filterRunningSagaId(ctx, sagaIds, clients)

	sagaIdPools := l.distributionSagaId(sagaIds, len(clients))
	logger.InfofX(ctx, "sagaIdPools=%v", sagaIdPools)
	for i, sagaIdPool := range sagaIdPools {
		request := proto.RecoverSagaRequest{
			SagaId: sagaIdPool,
		}
		response := proto.RecoverSagaResponse{}
		if err = clients[i].Call(ctx, common.RecoverSagaMethod, &request, &response); err != nil {
			logger.ErrorfX(ctx, "xClient call %v failed: client=%v request=%v err=%v", clients[i], common.RecoverSagaMethod, request, err)
		}
		if err = clients[i].Close(); err != nil {
			logger.ErrorfX(ctx, "xClient close failed: client=%v err=%v", clients[i], err)
		}
	}
	logger.InfofX(ctx, "saga recover over: sagaIds=%v", sagaIds)
}

func (l *RecoverLogic) filterRunningSagaId(ctx context.Context, sagaIds []int, clients []client.XClient) (result []int) {
	result = make([]int, 0)

	runningSagaIds := make(map[int]byte, 0)
	for _, xClient := range clients {
		request := proto.GetRunningSagaIdRequest{}
		response := proto.GetRunningSagaIdResponse{}
		if err := xClient.Call(ctx, common.GetRunningSagaIdMethod, &request, &response); err != nil {
			logger.ErrorfX(ctx, "xClient call %v failed: request=%v err=%v", common.GetRunningSagaIdMethod, request, err)
			continue
		}
		if len(response.SagaId) == 0 {
			logger.InfofX(ctx, "not found any running saga id: client=%v", xClient)
			continue
		}
		for _, sagaId := range response.SagaId {
			runningSagaIds[sagaId] = 0
		}
	}

	for _, sagaId := range sagaIds {
		if _, ok := runningSagaIds[sagaId]; ok {
			continue
		}
		result = append(result, sagaId)
	}

	return result
}

func (l *RecoverLogic) getClients(ctx context.Context, servers []*client.KVPair) (result []client.XClient) {
	result = make([]client.XClient, 0)
	for _, server := range servers {
		d, err := client.NewPeer2PeerDiscovery(server.Key, "")
		if err != nil {
			logger.ErrorfX(ctx, "client.NewPeer2PeerDiscovery failed: server=%v, err=%v", server, err)
			continue
		}
		xClient := client.NewXClient(config.GetString("SagaService.Name"), client.Failtry, client.RandomSelect, d, client.DefaultOption)
		result = append(result, xClient)
	}

	return result
}

func (l *RecoverLogic) distributionSagaId(sagaIds []int, poolNum int) map[int][]int {
	pools := make(map[int][]int)
	for i := 0; i < poolNum; i++ {
		pools[i] = make([]int, 0)
	}
	poolIndex := 0
	for _, sagaId := range sagaIds {
		pool := pools[poolIndex]
		pool = append(pool, sagaId)
		pools[poolIndex] = pool
		if poolIndex+1 == poolNum {
			poolIndex = 0
		} else {
			poolIndex++
		}
	}

	return pools
}

func (l *RecoverLogic) getSagaServices(ctx context.Context) (result []*client.KVPair, err error) {
	var d client.ServiceDiscovery
	switch config.GetString("Registry.Type") {
	case common.EtcdRegistry:
		d, err = etcdClient.NewEtcdDiscovery(config.GetString("SagaService.BasePath"), config.GetString("SagaService.Name"), config.GetStringSlice("Registry.Addr"), false, nil)
	case common.EtcdV3Registry:
		d, err = etcdClient.NewEtcdV3Discovery(config.GetString("SagaService.BasePath"), config.GetString("SagaService.Name"), config.GetStringSlice("Registry.Addr"), false, nil)
	case common.ZookeeperRegistry:
		d, err = client.NewZookeeperDiscovery(config.GetString("SagaService.BasePath"), config.GetString("SagaService.Name"), config.GetStringSlice("Registry.Addr"), nil)
	}
	if err != nil {
		return result, err
	}
	serviceByte, _ := json.Marshal(d.GetServices())
	logger.InfofX(ctx, "client.Services=%v", string(serviceByte))
	result = make([]*client.KVPair, 0)
	for _, kvPair := range d.GetServices() {
		if kvPair.Value != "group="+config.GetString("Registry.Group") {
			continue
		}
		result = append(result, kvPair)
	}

	return result, nil
}
