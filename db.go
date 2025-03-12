package kvdb

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ongniud/wal"
)

type DB struct {
	wal   *wal.WAL
	store *Store
	tid   uint64
	mu    sync.Mutex
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
		tid:   txId,
	}, nil
}

func (db *DB) getTid() uint64 {
	return atomic.AddUint64(&db.tid, 1)
}

func (db *DB) Update(fn func(tx *Transaction) error) error {
	txn, err := NewTransaction(db.getTid(), db.wal, db.store)
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
	txn, err := NewTransaction(db.getTid(), db.wal, db.store)
	if err != nil {
		return err
	}
	txn.state = TxnCommitted
	return fn(txn)
}

func (db *DB) LoadSnapshot() error {
	return nil
}

func (db *DB) Persist(snapshotPath string) error {
	tid := db.getTid()
	entry := &LogEntry{
		TxID: tid,
		Type: LogPersist,
	}
	line, err := entry.Encode()
	if err != nil {
		return err
	}
	pos, err := db.wal.Write(line)
	if err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file: %w", err)
	}
	defer file.Close()

	data := struct {
		Tid   uint64            `json:"tid"`
		Pos   []byte            `json:"pos"`
		Store map[string]string `json:"store"`
	}{
		Tid:   db.tid,
		Store: db.store.Snapshot(),
		Pos:   pos.Encode(),
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	return nil
}
