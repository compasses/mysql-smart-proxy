package server

import (
	"io"
	"net"

	"github.com/compasses/mysql-load-balancer/core/golog"
	"github.com/compasses/mysql-load-balancer/mysql"
)

type TransPipe struct {
	Src    io.ReadWriter
	Dst    io.ReadWriter
	Info   string
	ErrMsg chan string
	Quit   chan bool
	Cid    uint32
	Direct int // 0 from client, 1 from server
}

func (t *TransPipe) PipeStream() {
	//64K buffer
	buf := make([]byte, 0xffff)
	for {
		select {
		case <-t.Quit:
			golog.Info("Transport", "PipeStream quit ", t.Info, t.Cid, "Client Quit")
			return
		default:
			//golog.Info("Transport", "PipeStream", t.Info, t.Cid)
			n, err := t.Src.Read(buf)
			// if t.Direct == 1 && len(buf) > 4 && buf[4] == mysql.OK_HEADER {
			// 	golog.Info("Transport", "PipeStream server ping received need quit ", t.Info, t.Cid, "server Quit")
			// 	// t.ErrMsg <- "Client Quit"
			// 	// t.Quit <- true
			// 	// return
			// }
			//golog.Info("Transport", "PipeStream read ", t.Info, t.Cid, n)

			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				t.PipeError(err)
				return
			}
			sent := buf[:n]
			//process speical command from client
			if t.Direct == 0 && len(sent) >= 4 {
				cmd := sent[4]
				switch cmd {
				case mysql.COM_QUIT:
					//golog.Info("Transport", "PipeStream", t.Info, t.Cid, "Client Quit")
					t.ErrMsg <- "Client Quit"
					t.Quit <- true
					// t.Dst.Write([]byte{
					// 	0x01, //1 bytes long
					// 	0x00,
					// 	0x00,
					// 	0x00, //sequence
					// 	mysql.COM_PING})
					return
				}
			}
			// } else if (t.Direct == 1 && len(sent) > 4 )

			n, err = t.Dst.Write(sent)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				t.PipeError(err)
				return
			}
		}

	}
}

func (t *TransPipe) PipeError(err error) {
	if err != io.EOF {
		golog.Warn("Server", "PipeError", t.Info, t.Cid, err)
	}
	t.ErrMsg <- err.Error()
}
