// Copyright 2016 The MSP Authors. All rights reserved.
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
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/compasses/mysql-smart-proxy/backend"
	"github.com/compasses/mysql-smart-proxy/config"
	"github.com/compasses/mysql-smart-proxy/core/golog"
	"github.com/compasses/mysql-smart-proxy/mysql"
)

type Server struct {
	cfg      *config.Config
	addr     string
	user     string
	password string
	db       string

	counter  *Counter
	nodes    map[string]*backend.Node
	listener net.Listener
	running  bool
}

func (s *Server) parseNode(cfg config.NodeConfig) (*backend.Node, error) {
	var err error
	n := new(backend.Node)
	n.Cfg = cfg

	n.DownAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second
	err = n.ParseMaster(cfg.Master)

	err = n.ParseSlave(cfg.Slave)

	go n.CheckNode()

	return n, err
}

func (s *Server) parseNodes() error {
	cfg := s.cfg
	s.nodes = make(map[string]*backend.Node, len(cfg.Nodes))

	for _, v := range cfg.Nodes {
		if _, ok := s.nodes[v.Name]; ok {
			return fmt.Errorf("duplicate node [%s].", v.Name)
		}

		n, err := s.parseNode(v)
		if err != nil {
			golog.Error("Server", "parseNodes", "configure node has some error ", 0, err)
		}

		s.nodes[v.Name] = n
	}

	return nil
}

func NewServer(cfg *config.Config) (*Server, error) {
	s := new(Server)

	s.cfg = cfg
	s.counter = new(Counter)
	s.addr = cfg.Addr
	s.user = cfg.User
	s.password = cfg.Password

	if err := s.parseNodes(); err != nil {
		return nil, err
	}

	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return nil, err
	}

	golog.Info("server", "NewServer", "Server running", 0,
		"netProto",
		"tcp",
		"address",
		s.addr)
	return s, nil
}

func (s *Server) flushCounter() {
	for {
		time.Sleep(1 * time.Second)
		s.counter.FlushCounter()
	}
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	c := new(ClientConn)
	tcpConn := co.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(false)
	c.c = tcpConn

	c.pkg = mysql.NewPacketIO(tcpConn)
	c.proxy = s

	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	c.txConns = make(map[*backend.Node]*backend.BackendConn)

	c.closed = false

	return c
}

func (s *Server) onConn(c net.Conn) {
	s.counter.IncrClientConns()
	conn := s.newClientConn(c)

	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			golog.Error("server", "onConn", "error", 0,
				"remoteAddr", c.RemoteAddr().String(),
				"stack", string(buf),
			)
		}

		conn.Close()
		s.counter.DecrClientConns()
	}()

	if err := conn.Handshake(); err != nil {
		golog.Info("server", "onConn", err.Error(), 0)
		c.Close()
		s.counter.IncrErrLogTotal()
		return
	}

	conn.Run()
}

func (s *Server) handleSaveProxyConfig() error {
	err := config.WriteConfigFile(s.cfg)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Run() error {
	s.running = true

	// flush counter
	go s.flushCounter()
	for name, val := range s.nodes {
		golog.Info("Server", "Node ", "Info ", 0, name, val)
	}

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			golog.Error("server", "Run", err.Error(), 0)
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) DeleteSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.DeleteSlave(addr)
}

func (s *Server) AddSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.AddSlave(addr)
}

func (s *Server) UpMaster(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.UpMaster(addr)
}

func (s *Server) UpSlave(node string, addr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.UpSlave(addr)
}

func (s *Server) DownMaster(node, masterAddr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}
	return n.DownMaster(masterAddr, backend.ManualDown)
}

func (s *Server) DownSlave(node, slaveAddr string) error {
	n := s.GetNode(node)
	if n == nil {
		return fmt.Errorf("invalid node [%s].", node)
	}
	return n.DownSlave(slaveAddr, backend.ManualDown)
}

func (s *Server) GetNode(name string) *backend.Node {
	return s.nodes[name]
}
