package main

import (
	"encoding/json"
	"github.com/ongniud/wal"
)

// Transaction represents a transaction
type Transaction struct {
	ID      uint64
	Entries []LogEntry
	wal     *wal.WAL
}

// Write adds a write operation to the transaction
func (tx *Transaction) Write(key, value string) error {
	record := LogEntry{TxID: tx.ID, Type: LogUpdate, Key: key, Value: value}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = tx.wal.Write(data)
	if err != nil {
		return err
	}
	return tx.wal.Sync()
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	record := LogEntry{TxID: tx.ID, Type: LogCommit}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = tx.wal.Write(data)
	if err != nil {
		return err
	}
	return tx.wal.Sync()
}

// Abort aborts the transaction
func (tx *Transaction) Abort() error {
	record := LogEntry{TxID: tx.ID, Type: LogAbort}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = tx.wal.Write(data)
	if err != nil {
		return err
	}
	return tx.wal.Sync()
}
