package main

import (
	"context"
	"github.com/xuweijie4030/go-common/gosagarecover/proto"
	"go-saga-recover/logic"
)

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) Recover(ctx context.Context, request *proto.RecoverRequest, response *proto.RecoverResponse) (err error) {
	logic.NewRecoverLogic().Add(ctx)

	return nil
}
