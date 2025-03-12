package kvdb

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ongniud/wal"
)

type DB struct {
	wal   *wal.WAL
	store *Store
	txId  uint64
}

func NewDB(dir string) (*DB, error) {
	wlg, err := wal.Open(wal.Options{
		Directory:    dir,
		SegmentSize:  1 * wal.GB,
		SyncInterval: 1 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	store := NewStore()
	recovery, err := NewRecovery(store, wlg)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	if err := recovery.Recover(&wal.Position{}); err != nil {
		fmt.Println(err)
		return nil, err
	}
	txId := store.GetMaxTxnId()
	return &DB{
		wal:   wlg,
		store: store,
		txId:  txId,
	}, nil
}

func (db *DB) getTxnId() uint64 {
	return atomic.AddUint64(&db.txId, 1)
}

func (db *DB) Update(fn func(tx *Transaction) error) error {
	txn, err := NewTransaction(db.getTxnId(), db.wal, db.store)
	if err != nil {
		return err
	}
	if err := txn.Begin(); err != nil {
		return err
	}
	if err := fn(txn); err != nil {
		if err := txn.Abort(); err != nil {
			return err
		}
		return err
	}
	return txn.Commit()
}

func (db *DB) View(fn func(tx *Transaction) error) error {
	txn, err := NewTransaction(db.getTxnId(), db.wal, db.store)
	if err != nil {
		return err
	}
	txn.state = TxnCommitted
	return fn(txn)
}

func (db *DB) Persist() error {
	return nil
}
