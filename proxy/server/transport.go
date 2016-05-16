package server

import (
	"fmt"
	"io"
	"net"

	"github.com/compasses/mysql-load-balancer/backend"
	"github.com/compasses/mysql-load-balancer/core/golog"
	"github.com/compasses/mysql-load-balancer/mysql"
)

type Transport struct {
	Client    TransPipe
	Server    TransPipe
	Quit      chan bool
	backend   *backend.BackendConn
	clientend *ClientConn
}

type TransPipe struct {
	pipe      net.Conn
	info      string
	errMsg    chan string
	RoundTrip chan int
	quit      bool
	cid       uint32
	direct    int // 0 from client, 1 from server
}

const (
	readBuf      int = 1024
	readLargeBuf int = 1024 * 1024 * 16
)

func NewTransport(c *ClientConn) (*Transport, error) {
	//got backend connection
	backConn, err := c.GetBackendConn("node1")
	if err != nil {
		golog.Error("Transport", "NewTransport", "no backend connection available", c.connectionId, err.Error())
		return nil, err
	}
	backConn.UseDB("ESHOPDB16")
	t := new(Transport)

	t.Client = TransPipe{
		pipe:   c.c,
		info:   c.Info(),
		cid:    c.connectionId,
		direct: 0,
	}

	t.Server = TransPipe{
		pipe:   backConn.Conn.GetTCPConnect(),
		info:   backConn.Info(),
		cid:    backConn.Conn.ConnectionId(),
		direct: 1,
	}

	t.backend = backConn
	t.clientend = c

	return t, nil
}

func (trans *Transport) Run() {
	defer trans.backend.Close()
	golog.Info("Transport", "Run", "Start transfer", trans.Client.cid, "backend cid", trans.Server.cid, trans.Client.info, trans.Server.info)

	for {
		data, err := trans.Client.ReadClientRaw()
		if err != nil {
			golog.Warn("Transport", "Run", "client error", trans.Client.cid, err.Error())
			return
		}
		isQuery := false

		if len(data) > 4 {
			cmd := data[4]
			switch cmd {
			case mysql.COM_QUIT:
				golog.Info("Transport", "Run", "client quit", trans.Client.cid)
				return
			case mysql.COM_PING:
				trans.clientend.writeOK(nil)
				golog.Warn("Transport", "Run", "client ping", uint32(cmd))
				continue
			// case mysql.COM_INIT_DB:
			// 	if err := trans.clientend.useDB(hack.String(data)); err != nil {
			// 		return //err
			// 	} else {
			// 		trans.clientend.writeOK(nil)
			// 	}
			// 	golog.Warn("Transport", "Run", "client change DB", uint32(cmd), string(data[5:]))
			// 	continue
			case mysql.COM_QUERY:
				isQuery = true
			}
			golog.Debug("Transport", "Run", "client command", uint32(cmd), string(data[5:]))
		}

		//send to server
		err = trans.Server.Write(data)
		if err != nil {
			golog.Warn("Transport", "Run", "server write error", trans.Server.cid, err.Error())
			return
		}

		//read response from server
		data, err = trans.Server.ReadServerRaw(false)
		golog.Debug("Transport", "Run", "server read ", trans.Server.cid, data)

		if err != nil {
			golog.Warn("Transport", "Run", "server read error", trans.Server.cid, err.Error())
			return
		}

		if isQuery && data[4] != mysql.OK_HEADER {
			result, err := trans.Server.ReadServerRaw(true)
			if err != nil {
				golog.Warn("Transport", "Run", "server read error", trans.Server.cid, err.Error())
				return
			}
			data = append(data, result...)
			golog.Debug("Transport", "Run", "nest server read ", trans.Server.cid, data)
		}

		// send to client
		err = trans.Client.Write(data)
		if err != nil {
			golog.Warn("Transport", "Run", "client write error", trans.Client.cid, err.Error())
			return
		}
	}
}

func (trans *TransPipe) ReadHeader() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(trans.pipe, header); err != nil {
		return nil, mysql.ErrBadConn
	}
	return header[:], nil
}

func (trans *TransPipe) ReadClientRaw() ([]byte, error) {
	header, err := trans.ReadHeader()
	if err != nil {
		return nil, mysql.ErrBadConn
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(trans.pipe, data); err != nil {
		return nil, mysql.ErrBadConn
	} else {
		if length < mysql.MaxPayloadLen {
			return append(header[:], data...), nil
		} else {
			fmt.Errorf("invalid payload length %d", length)
			return nil, mysql.ErrBadConn
		}
		//
		// var buf []byte
		// buf, err = pipe.ReadClientData()
		// if err != nil {
		// 	return nil, mysql.ErrBadConn
		// } else {
		// 	return append(data, buf...), nil
		// }
	}
}

func (trans *TransPipe) ReadServerRaw(isNested bool) ([]byte, error) {
	header, err := trans.ReadHeader()
	if err != nil {
		return nil, mysql.ErrBadConn
	}
	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(trans.pipe, data); err != nil {
		return nil, mysql.ErrBadConn
	} else {
		if data[0] == mysql.OK_HEADER && !isNested {
			return append(header[:], data...), nil
		} else if data[0] == mysql.EOF_HEADER && len(data) <= 5 {
			return append(header[:], data...), nil
		} else {
			//need continue read until EOF
			var buf []byte
			buf, err = trans.ReadServerRaw(true)
			if err != nil {
				return nil, mysql.ErrBadConn
			} else {
				header = append(header[:], data...)
				return append(header, buf...), nil
			}
		}
		// if length >= mysql.MaxPayloadLen {
		// 	return append(header[:], data...), nil
		// } else {
		// 	fmt.Errorf("invalid payload length %d", length)
		// 	return nil, mysql.ErrBadConn
		// }
		//
		// var buf []byte
		// buf, err = pipe.ReadClientData()
		// if err != nil {
		// 	return nil, mysql.ErrBadConn
		// } else {
		// 	return append(data, buf...), nil
		// }
	}
}

func (trans *TransPipe) Write(data []byte) error {
	_, err := trans.pipe.Write(data)
	return err
}

// func (trans *Transport) Start() {
// 	defer trans.backend.Close()
//
// 	go func() {
// 		golog.Info("Transport", "Transform", "Start transfer", trans.Client.cid, "backend cid", trans.Server.cid, trans.Client.info, trans.Server.info)
//
// 		for {
// 			if !trans.Client.quit {
// 				go trans.Client.PipeStream()
// 			}
// 			var toServer int
// 			select {
// 			case toServer = <-trans.Client.RoundTrip:
// 				// case <-time.After(time.Second * 1):
// 				// 	golog.Info("Transport", "Transform", "Client Transfer more than 1 second", trans.Client.cid, "backend cid", trans.Server.cid)
// 				// 	break
// 			}
//
// 			if toServer == 0 {
// 				trans.Client.quit = true
// 			} else {
// 				trans.Server.quit = false
// 			}
//
// 			golog.Info("Transport", "Transform", "Client Transfer ", trans.Client.cid, toServer)
//
// 			if !trans.Client.quit {
// 				go trans.Server.PipeStream()
// 				var sentBack int
// 				select {
// 				case sentBack = <-trans.Server.RoundTrip:
// 					// case <-time.After(time.Second * 2):
// 					// 	golog.Info("Transport", "Transform", "Server Transfer more than 1 second", trans.Client.cid, "backend cid", trans.Server.cid)
// 					// 	break
// 				}
// 				golog.Info("Transport", "Transform", "Server Transfer ", trans.Server.cid, sentBack)
// 				if sentBack == 0 {
// 					trans.Server.quit = true
// 				}
// 			}
//
// 			if trans.Client.quit {
// 				break
// 			}
// 		}
// 		trans.Quit <- true
// 	}()
//
// 	<-trans.Quit
// 	golog.Info("Transport", "Transform", "Finish", trans.Client.cid, "backend cid", trans.Server.cid)
// }

// func (t *TransPipe) PipeStream() {
// 	//1k buffer
// 	buf := make([]byte, readBuf)
//
// 	n, err := t.src.Read(buf)
//
// 	if err != nil {
// 		t.PipeError(err)
// 		return
// 	}
// 	var sent []byte
// 	totalLen := n
// 	if n == readBuf {
// 		newBuf := make([]byte, readLargeBuf)
// 		newBuf = buf[:]
// 		for {
// 			golog.Warn("Server", "PipeStream", "more data need to read", t.cid, totalLen)
// 			buf = make([]byte, readBuf)
// 			n, err = t.src.Read(buf)
// 			totalLen += n
// 			newBuf = append(newBuf, buf[:n]...)
// 			if err != nil {
// 				t.PipeError(err)
// 				return
// 			}
// 			if n < readBuf {
// 				sent = newBuf[:totalLen]
// 				break
// 			}
// 		}
// 	} else {
// 		sent = buf[:totalLen]
// 	}
//
// 	if t.direct == 0 && len(sent) >= 4 {
// 		cmd := sent[4]
// 		switch cmd {
// 		case mysql.COM_QUIT:
// 			golog.Info("Transport", "PipeStream Got Client Quit command", t.info, t.cid, "Client Quit")
// 			t.RoundTrip <- 0
// 			return
// 		}
// 		golog.Info("Transport", "PipeStream Got Client command", t.info, t.cid, string(sent[5:]))
// 	}
//
// 	n, err = t.dst.Write(sent)
// 	if err != nil {
// 		t.PipeError(err)
// 		return
// 	}
//
// 	t.RoundTrip <- n
// }

func (t *TransPipe) PipeError(err error) {
	if err != io.EOF {
		golog.Warn("Server", "PipeError", t.info, t.cid, err)
	}
	t.RoundTrip <- 0
}
