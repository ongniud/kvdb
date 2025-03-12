package main

import (
	"encoding/json"
	"github.com/ongniud/wal"
)

// RecoveryManager manages the recovery process
type RecoveryManager struct {
	wal *wal.WAL
}

// NewRecoveryManager creates a new RecoveryManager instance
func NewRecoveryManager(wal *wal.WAL) (*RecoveryManager, error) {
	return &RecoveryManager{
		wal: wal,
	}, nil
}

func (rm *RecoveryManager) do(entry *LogEntry) error {
	txLogs :=
}

func (rm *RecoveryManager) Recover(pos *wal.Position, fn func(entry *LogEntry) error) error {
	rdr, err := rm.wal.NewReader(pos)
	if err != nil {
		return err
	}

	for {
		data, err := rdr.Next()
		if err != nil {
			return err
		}

		entry, err := parseLogEntry(data)
		if err != nil {
			return err
		}

		if err := fn(entry); err != nil {
			return err
		}

		txLog[entry.TxID] = append(txLog[entry.TxID], entry)

		switch entry.Type {
		case LogBegin:
			transactionStatus[entry.TxID] = false
		case LogUpdate:
			if _, exists := transactionStatus[entry.TxID]; exists {
				rm.DataStore.Apply(entry.Key, entry.Value)
			}
		case LogCommit:
			transactionStatus[entry.TxID] = true
		case LogAbort:
			delete(transactionStatus, entry.TxID)
		}
	}

	for txID, committed := range transactionStatus {
		if !committed {
			rm.undoTransaction(txID, txLog[txID])
		}
	}

	return nil
}

func (rm *RecoveryManager) undoTransaction(transactionID uint64, records []LogEntry) {
	for i := len(records) - 1; i >= 0; i-- {
		entry := records[i]
		if entry.Type == LogUpdate {
			rm.DataStore.Rollback(entry.Key)
		}
	}
}

// 关闭恢复管理器
func (rm *RecoveryManager) Close() error {
	return rm.WAL.Close()
}

// 解析日志条目
func parseLogEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	err := json.Unmarshal(data, &entry)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}
