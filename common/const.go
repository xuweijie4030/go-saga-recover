package common

const SagaRunningStatus = 0

const (
	EtcdRegistry      = "etcd"
	EtcdV3Registry    = "etcdV3"
	ZookeeperRegistry = "zookeeper"
)

const (
	GetRunningSagaIdMethod = "GetRunningSagaId"
	RecoverSagaMethod      = "RecoverSaga"
)
