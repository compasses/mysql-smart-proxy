package server

import (
	"io"
	"net"

	"github.com/compasses/mysql-load-balancer/backend"
	"github.com/compasses/mysql-load-balancer/core/golog"
	"github.com/compasses/mysql-load-balancer/mysql"
)

type Transport struct {
	Client    TransPipe
	Server    TransPipe
	RoundTrip chan int
	Quit      chan bool
	backend   *backend.BackendConn
}

type TransPipe struct {
	src    net.Conn
	dst    net.Conn
	tline  *Transport
	info   string
	errMsg chan string
	quit   chan bool
	cid    uint32
	direct int // 0 from client, 1 from server
}

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
		src:    c.c,
		dst:    backConn.Conn.GetTCPConnect(),
		tline:  t,
		info:   c.Info(),
		cid:    c.connectionId,
		quit:   make(chan bool),
		direct: 0,
	}

	t.Server = TransPipe{
		src:    backConn.Conn.GetTCPConnect(),
		dst:    c.c,
		tline:  t,
		info:   backConn.Info(),
		cid:    backConn.Conn.ConnectionId(),
		quit:   make(chan bool),
		direct: 1,
	}

	t.backend = backConn
	t.RoundTrip = make(chan int)
	t.Quit = make(chan bool)

	return t, nil
}

func (trans *Transport) Start() {
	defer trans.backend.Close()
	go func() {
		golog.Info("Transport", "Transform", "Start transfer", trans.Client.cid, "backend cid", trans.Server.cid)
		for {
			go trans.Client.PipeStream()
			sent := <-trans.RoundTrip
			if sent > 0 {
				go trans.Server.PipeStream()
				sent = <-trans.RoundTrip
				if sent <= 0 {
					golog.Info("Transport", "Start", "server transport end", trans.Server.cid)
				}
			} else {
				golog.Info("Transport", "Start", "client transport end", trans.Client.cid)
				break
			}
		}
		trans.Quit <- true
	}()

	<-trans.Quit
	golog.Info("Transport", "Transform", "Finish", trans.Client.cid, "backend cid", trans.Server.cid)
}

func (t *TransPipe) PipeStream() {
	//16M buffer
	buf := make([]byte, 1024*1024*16)
	n, err := t.src.Read(buf)

	if err != nil {
		t.PipeError(err)
		return
	}
	sent := buf[:n]
	if t.direct == 0 && len(sent) >= 4 {
		cmd := sent[4]
		switch cmd {
		case mysql.COM_QUIT:
			golog.Info("Transport", "PipeStream Got Client Quit command", t.info, t.cid, "Client Quit")
			t.tline.RoundTrip <- 0
			return
		}
	}
	n, err = t.dst.Write(sent)
	if err != nil {
		t.PipeError(err)
		return
	}

	t.tline.RoundTrip <- n
}

func (t *TransPipe) PipeError(err error) {
	if err != io.EOF {
		golog.Warn("Server", "PipeError", t.info, t.cid, err)
	}
	t.tline.RoundTrip <- 0
}
