package kvdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ongniud/wal"
)

// DB 封装 DataStore 并屏蔽事务细节
type DB struct {
	wal   *wal.WAL
	store *Store
	txId  uint64
	mu    sync.Mutex
}

// NewDB 创建一个新的 DB 实例
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

func (db *DB) GetTxnId() uint64 {
	a := atomic.AddUint64(&db.txId, 1)
	return a
}

// Update 提供可写事务，事务提交或回滚由 DB 自动处理
func (db *DB) Update(fn func(tx *Transaction) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	txn, err := NewTransaction(db.GetTxnId(), db.wal, db.store)
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

// View 提供只读事务
func (db *DB) View(fn func(tx *Transaction) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	txn, err := NewTransaction(db.GetTxnId(), db.wal, db.store)
	if err != nil {
		return err
	}
	// 事务默认是 Active，但不允许修改数据
	txn.state = TxnCommitted
	return fn(txn)
}

// Get 直接从 DataStore 读取数据（非事务性）
func (db *DB) Get(key string) (string, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.store.Get(key)
}
