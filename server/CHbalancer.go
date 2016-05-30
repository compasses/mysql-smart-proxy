package server

import (
	"errors"

	"stathat.com/c/consistent"
)

func (n *Node) InitCHBalancer() {
	if n.HashCircle == nil {
		n.HashCircle = consistent.New()
	}
	var strs []string

	if n.Master != nil {
		strs = append(strs, n.Master.Addr())
	}

	for _, sl := range n.Slave {
		if sl != nil {
			strs = append(strs, sl.Addr())
		}
	}
	n.HashCircle.Set(strs)
}

func (n *Node) GetConsistenHashConn(key string) (co *BackendConn, err error) {
	str, err := n.HashCircle.Get(key)
	if err != nil {
		return nil, err
	}

	db, err := n.GetDBFromAddr(str)
	if err != nil {
		return nil, err
	}
	return db.GetConn()
}

func (n *Node) GetDBFromAddr(addr string) (db *DB, err error) {
	if n.Master != nil && n.Master.Addr() == addr {
		return n.Master, nil
	}
	for _, sl := range n.Slave {
		if sl != nil && sl.Addr() == addr {
			return sl, nil
		}
	}
	return nil, errors.New("not found db " + addr)
}
