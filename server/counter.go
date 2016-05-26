// Copyright 2015 The MSP Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"sync/atomic"

	"github.com/compasses/mysql-smart-proxy/core/golog"
)

type NodeInfo struct {
	//ID
	ConnectIdle  int64
	ConnectCache int64
}

type Counter struct {
	OldClientQPS    int64
	OldErrLogTotal  int64
	OldSlowLogTotal int64

	ClientConns  int64
	ClientQPS    int64
	ErrLogTotal  int64
	SlowLogTotal int64
	TxConnNum    int64
	ConnectInUse int64

	NodeInfo
}

// simplily count all
func (counter *Counter) FlushDBConnectionInfo(totalCache, totalIdel int64) {
	atomic.StoreInt64(&counter.NodeInfo.ConnectIdle, totalIdel)
	atomic.StoreInt64(&counter.NodeInfo.ConnectCache, totalCache)
}

func (counter *Counter) IncrConnectInUse() {
	atomic.AddInt64(&counter.ConnectInUse, 1)
}

func (counter *Counter) DecrConnectInUse() {
	atomic.AddInt64(&counter.ConnectInUse, -1)
}

func (counter *Counter) IncrConnectIdle() {
	atomic.AddInt64(&counter.NodeInfo.ConnectIdle, 1)
}

func (counter *Counter) DecrConnectIdle() {
	atomic.AddInt64(&counter.NodeInfo.ConnectIdle, -1)
}

func (counter *Counter) IncrConnectCache() {
	atomic.AddInt64(&counter.NodeInfo.ConnectCache, 1)
}

func (counter *Counter) DecrConnectCache() {
	atomic.AddInt64(&counter.NodeInfo.ConnectCache, -1)
}

func (counter *Counter) IncrClientConns() {
	atomic.AddInt64(&counter.ClientConns, 1)
}

func (counter *Counter) DecrClientConns() {
	atomic.AddInt64(&counter.ClientConns, -1)
}

func (counter *Counter) IncrClientQPS() {
	atomic.AddInt64(&counter.ClientQPS, 1)
}

func (counter *Counter) IncrTxConn() {
	atomic.AddInt64(&counter.TxConnNum, 1)
}

func (counter *Counter) DecrTxConn() {
	atomic.AddInt64(&counter.TxConnNum, -1)
}

func (counter *Counter) IncrErrLogTotal() {
	atomic.AddInt64(&counter.ErrLogTotal, 1)
}

func (counter *Counter) IncrSlowLogTotal() {
	atomic.AddInt64(&counter.SlowLogTotal, 1)
}

//flush the count per second
func (counter *Counter) FlushCounter() {
	golog.RunInfo("Proxy Info ==> QPS:%d, Client Connect:%d, Error:%d, Slow:%d Connection Info ==> Used:%d, TxConn:%d, Cached:%d, Idle:%d", counter.OldClientQPS, counter.ClientConns, counter.ErrLogTotal, counter.SlowLogTotal,
		counter.ConnectInUse, counter.TxConnNum, counter.NodeInfo.ConnectCache, counter.NodeInfo.ConnectIdle)

	atomic.StoreInt64(&counter.OldClientQPS, counter.ClientQPS)
	atomic.StoreInt64(&counter.OldErrLogTotal, counter.ErrLogTotal)
	atomic.StoreInt64(&counter.OldSlowLogTotal, counter.SlowLogTotal)

	atomic.StoreInt64(&counter.ClientQPS, 0)
}
