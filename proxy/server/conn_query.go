// Copyright 2016 The kingshard Authors. All rights reserved.
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
	"github.com/compasses/mysql-load-balancer/backend"
	"github.com/flike/kingshard/core/golog"
)

func (c *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	co, err = n.GetMasterConn()
	if err != nil {
		golog.Error("Server", "getBackendConn", err.Error(), 0)
		return
	}
	// if !c.isInTransaction() {
	// 	if fromSlave {
	// 		co, err = n.GetSlaveConn()
	// 		if err != nil {
	// 			co, err = n.GetMasterConn()
	// 		}
	// 	} else {
	// 		co, err = n.GetMasterConn()
	// 	}
	// 	if err != nil {
	// 		golog.Error("server", "getBackendConn", err.Error(), 0)
	// 		return
	// 	}
	// } else {
	// 	var ok bool
	// 	co, ok = c.txConns[n]
	//
	// 	if !ok {
	// 		if co, err = n.GetMasterConn(); err != nil {
	// 			return
	// 		}
	//
	// 		if !c.isAutoCommit() {
	// 			if err = co.SetAutoCommit(0); err != nil {
	// 				return
	// 			}
	// 		} else {
	// 			if err = co.Begin(); err != nil {
	// 				return
	// 			}
	// 		}
	//
	// 		c.txConns[n] = co
	// 	}
	// }
	//
	// if err = co.UseDB(c.db); err != nil {
	// 	return
	// }
	//
	// if err = co.SetCharset(c.charset); err != nil {
	// 	return
	// }

	return
}
