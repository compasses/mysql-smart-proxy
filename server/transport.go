package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/compasses/mysql-smart-proxy/core/golog"
	"github.com/compasses/mysql-smart-proxy/core/hack"
	"github.com/compasses/mysql-smart-proxy/mysql"
)

type Transport struct {
	Client     *TransPipe
	dbSelected bool
	clientend  *ClientConn
}

type TransPipe struct {
	pipe net.Conn
	info string
	cid  uint32
}

func NewTransport(c *ClientConn) (*Transport, error) {

	t := new(Transport)

	t.Client = &TransPipe{
		pipe: c.c,
		info: c.Info(),
		cid:  c.connectionId,
	}
	t.dbSelected = false
	t.clientend = c
	return t, nil
}

func (trans *Transport) ExecServerEnd(data []byte, needResponse bool) error {
	//got backend connection
	var backConn *BackendConn
	var err error
	if trans.clientend.txConn != nil {
		backConn = trans.clientend.txConn
	} else {
		// just use the default node
		backConn, err = trans.clientend.GetBackendConn("node1")
		trans.clientend.proxy.counter.IncrConnectInUse()
	}

	if err != nil {
		golog.Error("Transport", "NewTransport", "no backend connection available", trans.clientend.connectionId, err.Error())
		return err
	}

	defer func() {
		if trans.clientend.txConn == nil {
			// not in transaction
			backConn.Close()
			trans.clientend.proxy.counter.DecrConnectInUse()
		}
	}()

	server := TransPipe{
		pipe: backConn.Conn.GetTCPConnect(),
		info: backConn.Info(),
		cid:  backConn.Conn.ConnectionId(),
	}

	golog.Info("Transport", "ExecServerEnd", "Start transfer", server.cid, server.info, "Client Data:", hack.String(data))

	var isQuery bool
	queryStr := ""
	cmd := data[4]
	switch cmd {
	case mysql.COM_QUERY:
		isQuery = true
		queryStr = hack.String(data[5:])
		if strings.ToLower(queryStr) == "begin" {
			//it's a transaction
			if trans.clientend.txConn == nil {
				trans.clientend.txConn = backConn
				trans.clientend.proxy.counter.IncrTxConn()
			}
		}
		break
	case mysql.COM_STMT_PREPARE:
		//transaction begin
		if trans.clientend.txConn == nil {
			trans.clientend.txConn = backConn
			trans.clientend.proxy.counter.IncrTxConn()
		}
		break
	}

	if len(trans.clientend.db) > 0 {
		golog.Info("Transport", "NewTransport", "Select DB", trans.Client.cid, trans.clientend.db)
		backConn.UseDB(trans.clientend.db)
		trans.dbSelected = true
	} else if trans.dbSelected == false {
		if isQuery {
			golog.Warn("Transport", "NewTransport", "No DB select for client", trans.clientend.connectionId, hack.String(data[5:]))
		}
	}

	golog.Info("Transport", "Run", "client command", server.cid, uint32(cmd), hack.String(data[5:]))

	var tickNow time.Time
	if isQuery && trans.clientend.proxy.cfg.LogSql == 1 {
		tickNow = time.Now()
	}

	//send to server
	err = server.Write(data)
	if err != nil {
		golog.Warn("Transport", "Run", "server write error", server.cid, err.Error())
		trans.clientend.proxy.counter.IncrErrLogTotal()
		return err
	}

	//no need response
	if !needResponse {
		return nil
	}
	//read response from server
	data, err = server.ReadServerRaw(cmd)
	golog.Debug("Transport", "Run", "server read ", server.cid, data)

	if err != nil {
		golog.Warn("Transport", "Run", "server read error", server.cid, err.Error())
		trans.clientend.proxy.counter.IncrErrLogTotal()
		return err
	}

	if isQuery && trans.clientend.proxy.cfg.LogSql == 1 {
		elapsed := time.Since(tickNow).Nanoseconds() / 1000000
		if elapsed >= trans.clientend.proxy.cfg.SlowLogTime {
			trans.clientend.proxy.counter.IncrSlowLogTotal()
			golog.OutputSql("Slow Query", queryStr+" time:%vms", elapsed)
		}
	}
	// send to client
	err = trans.Client.Write(data)
	if err != nil {
		golog.Warn("Transport", "Run", "client write error", trans.Client.cid, err.Error())
		trans.clientend.proxy.counter.IncrErrLogTotal()
		return err
	}

	return nil
}

func (trans *Transport) Run() {
	golog.Info("Transport", "Run", "Start transfer", trans.Client.cid, trans.Client.info)
	defer func() {
		if trans.clientend.txConn != nil {
			trans.clientend.txConn.Close()
			trans.clientend.proxy.counter.DecrTxConn()
			trans.clientend.proxy.counter.DecrConnectInUse()
		}
	}()

	for {
		data, err := trans.Client.ReadClientRaw()
		if data == nil && err == nil {
			//client reset connect
			return
		}

		if err != nil {
			golog.Warn("Transport", "Run", "client error", trans.Client.cid, err.Error())
			trans.clientend.proxy.counter.IncrErrLogTotal()
			return
		}
		trans.clientend.proxy.counter.IncrClientQPS()
		if len(data) > 4 {
			cmd := data[4]
			switch cmd {
			case mysql.COM_QUIT:
				golog.Info("Transport", "Run", "client quit", trans.Client.cid)
				return
			case mysql.COM_PING:
				trans.Client.WriteOK()
				golog.Warn("Transport", "Run", "client ping", uint32(cmd))
				continue
			case mysql.COM_INIT_DB:
				trans.clientend.db = hack.String(data[5:])
				// if err := trans.backend.UseDB(hack.String(data[5:])); err != nil {
				// 	golog.Warn("Transport", "Run", "Use DB error", trans.Client.cid, err.Error(), hack.String(data[5:]))
				// 	trans.clientend.proxy.counter.IncrErrLogTotal()
				// 	return
				// }
				trans.Client.WriteOK()
				continue
			case mysql.COM_STMT_CLOSE:
			case mysql.COM_STMT_SEND_LONG_DATA:
				err := trans.ExecServerEnd(data, false)
				if err != nil {
					return
				}
				continue
			default:
				err := trans.ExecServerEnd(data, true)
				if err != nil {
					return
				}
			}
		} else {
			golog.Warn("Transport", "Run", "client error data ", trans.Client.cid, data)
			trans.clientend.proxy.counter.IncrErrLogTotal()
			return

		}
	}
}

func (trans *TransPipe) ReadPacket() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(trans.pipe, header); err != nil {
		if err == io.EOF {
			golog.Warn("Transport", "Read Packet", "Receive EOF", trans.cid)
			return nil, nil
		}
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
		}
		var buf []byte
		buf, err = trans.ReadPacket()
		if err != nil {
			return nil, mysql.ErrBadConn
		} else {
			header = append(header[:], data...)
			return append(header, buf...), nil
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
	return trans.ReadPacket()
}

func (trans *TransPipe) ReadServerColumns() ([]byte, error) {
	//just read packet
	var result []byte
	for {
		data, err := trans.ReadPacket()
		if err != nil {
			return nil, err
		}

		// EOF Packet
		if mysql.IsEOFPacket(data[4:]) {
			result = append(result, data...)
			return result, nil
		}
		result = append(result, data...)
	}
}

func (trans *TransPipe) ReadServerRows() ([]byte, error) {
	//now just same to read columns
	return trans.ReadServerColumns()
}

func (trans *TransPipe) ReadServerDefault() ([]byte, error) {
	data, err := trans.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[4] == mysql.OK_HEADER {
		return data, nil
	} else if data[4] == mysql.ERR_HEADER {
		golog.Error("Transport", "Read Server Default", "got error response", trans.cid, hack.String(data[5:]))
		return data, nil //errors.New(hack.String(data[5:]))
	}

	// must be a result set
	//get column count
	_, _, n := mysql.LengthEncodedInt(data[4:])
	if n-len(data[4:]) != 0 {
		return nil, mysql.ErrMalformPacket
	}
	//read result columns
	cols, err := trans.ReadServerColumns()
	if err != nil {
		return nil, err
	}

	//read result rows
	rows, err := trans.ReadServerRows()
	if err != nil {
		return nil, err
	}
	data = append(data, cols...)
	return append(data, rows...), nil
}

func (trans *TransPipe) ReadServerPrepareResponse() ([]byte, error) {
	// 	* `COM_STMT_PREPARE OK packet`_
	// * if num-params > 0
	//
	//   * num-params * `Column Definition`_
	//   * `EOF packet`_
	//
	// * if num-columns > 0
	//
	//   * num-colums * `Column Definition`_
	//   * `EOF packet`_
	// payload:
	// 1              [00] OK
	// 4              statement-id
	// 2              num-columns
	// 2              num-params
	// 1              [00] filler
	// 2              warning count
	data, err := trans.ReadPacket()
	if err != nil {
		return nil, err
	}

	if data[4] != mysql.OK_HEADER {
		return data, nil
	}
	if len(data[4:]) < 9 {
		return data, errors.New("prepare statement response error.")
	}
	//data[5,6,7,8] statement-id
	//length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	numColumns := int(uint32(data[9]) | uint32(data[10])<<8)
	numParams := int(uint32(data[11]) | uint32(data[12])<<8)

	if numColumns > 0 {
		//read result columns
		cols, err := trans.ReadServerColumns()
		if err != nil {
			return nil, err
		}
		data = append(data, cols...)
	}

	if numParams > 0 {
		//read result rows
		rows, err := trans.ReadServerRows()
		if err != nil {
			return nil, err
		}
		data = append(data, rows...)
	}

	return data, nil
}

func (trans *TransPipe) ReadServerRaw(cmd byte) ([]byte, error) {
	switch cmd {
	case mysql.COM_STMT_PREPARE:
		return trans.ReadServerPrepareResponse()
	default:
		return trans.ReadServerDefault()
	}
}

func (trans *TransPipe) Write(data []byte) error {
	_, err := trans.pipe.Write(data)
	return err
}

func (trans *TransPipe) WriteOK() error {
	var data [11]byte
	data[0] = 0x07
	data[3] = 0x01
	data[7] = 0x02
	_, err := trans.pipe.Write(data[:])
	return err
}
