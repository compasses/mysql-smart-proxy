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
	"bytes"
	"encoding/binary"
	"net"
	"runtime"
	"sync"

	"github.com/compasses/mysql-load-balancer/backend"
	"github.com/compasses/mysql-load-balancer/core/golog"
	"github.com/compasses/mysql-load-balancer/mysql"
)

//client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg *mysql.PacketIO

	c net.Conn

	proxy *Server

	capability uint32

	connectionId uint32

	status    uint16
	collation mysql.CollationId
	charset   string

	user string
	db   string

	salt []byte

	txConns map[*backend.Node]*backend.BackendConn

	closed       bool
	lastInsertId int64
	affectedRows int64
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func (c *ClientConn) Handshake() error {
	if err := c.writeInitialHandshake(); err != nil {
		golog.Error("server", "Handshake", err.Error(),
			c.connectionId, "msg", "send initial handshake error")
		return err
	}

	if err := c.readHandshakeResponse(); err != nil {
		golog.Error("server", "readHandshakeResponse",
			err.Error(), c.connectionId,
			"msg", "read Handshake Response error")

		c.writeError(err)

		return err
	}

	if err := c.writeOK(nil); err != nil {
		golog.Error("server", "readHandshakeResponse",
			"write ok fail",
			c.connectionId, "error", err.Error())
		return err
	}

	c.pkg.Sequence = 0

	return nil
}

func (c *ClientConn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.closed = true

	return nil
}

func (c *ClientConn) Info() string {
	return c.c.LocalAddr().String() + " => " + c.c.RemoteAddr().String()
}

func (c *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *ClientConn) readRaw() ([]byte, error) {
	return c.pkg.ReadRawBytes()
}

func (c *ClientConn) writeRaw(data []byte) error {
	return c.pkg.WriteRawBytes(data)
}

func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) readHandshakeResponse() error {
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	checkAuth := mysql.CalcPassword(c.salt, []byte(c.proxy.cfg.Password))
	if c.user != c.proxy.cfg.User || !bytes.Equal(auth, checkAuth) {
		golog.Error("ClientConn", "readHandshakeResponse", "error", 0,
			"auth", auth,
			"checkAuth", checkAuth,
			"client_user", c.user,
			"config_set_user", c.proxy.cfg.User,
			"passworld", c.proxy.cfg.Password)
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	var db string
	if c.capability&mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1

	}

	if err := c.useDB(db); err != nil {
		return err
	}

	return nil
}

func (c *ClientConn) Run() {
	//crash recover
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			golog.Error("ClientConn", "Run",
				err.Error(), 0,
				"stack", string(buf))
		}
		c.Close()
	}()

	trans, err := NewTransport(c)
	if err != nil {
		golog.Error("ClientConn", "Run",
			err.Error(), 0)
		return
	}

	trans.Run()
	golog.Error("ClientConn", "Run", "Transport Finish............", c.connectionId)
}

func (c *ClientConn) useDB(db string) error {
	c.db = db
	return nil
}

func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacketBatch(total, data, direct)
}
