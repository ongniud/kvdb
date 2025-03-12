package kvdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/ongniud/wal"
)

// Recovery manages the recovery process
type Recovery struct {
	store *Store
	wal   *wal.WAL
}

// NewRecovery creates a new RecoveryManager instance
func NewRecovery(store *Store, wal *wal.WAL) (*Recovery, error) {
	return &Recovery{
		store: store,
		wal:   wal,
	}, nil
}

// Recover replays the WAL and processes each log entry using the provided function
func (rm *Recovery) recover(pos *wal.Position, fn func(entry *LogEntry) error) error {
	rdr, err := rm.wal.NewReader(pos)
	if err != nil {
		return err
	}

	for {
		data, err := rdr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
			return err
		}
		entry, err := parseLogEntry(data)
		if err != nil {
			panic(err)
			return err
		}
		if err := fn(entry); err != nil {
			panic(err)
			return err
		}
	}

	return nil
}

func (rm *Recovery) Recover(pos *wal.Position) error {
	txLogs := make(map[uint64][]*LogEntry) // Stores transaction logs
	txStates := make(map[uint64]TxnState)  // Tracks transaction states

	if err := rm.recover(pos, func(entry *LogEntry) error {
		fmt.Println("entry:", entry)
		//return nil

		txID := entry.TxID
		rm.store.SetMaxTxnId(txID)
		// Ensure a valid state transition
		switch entry.Type {
		case LogBegin:
			if _, exists := txStates[txID]; exists {
				log.Printf("transaction %d is exist", txID)
				return nil
			}
			txStates[txID] = TxnActive
		case LogUpdate, LogDelete:
			if state, exists := txStates[txID]; !exists || state != TxnActive {
				log.Printf("invalid update: transaction %d is not active", txID)
				return nil
			}
			txLogs[txID] = append(txLogs[txID], entry)
		case LogCommit:
			if state, exists := txStates[txID]; !exists || state != TxnActive {
				log.Printf("invalid update: transaction %d is not active", txID)
				return nil
			}
			for _, logEntry := range txLogs[txID] {
				if logEntry.Type == LogUpdate {
					rm.store.Put(logEntry.Key, logEntry.Value)
				}
				if logEntry.Type == LogDelete {
					rm.store.Delete(logEntry.Key)
				}
			}
			delete(txLogs, txID)
			delete(txStates, txID)
		case LogAbort:
			if state, exists := txStates[txID]; !exists || state != TxnActive {
				log.Printf("invalid update: transaction %d is not active", txID)
				return nil
			}
			delete(txLogs, txID)
			delete(txStates, txID)
		default:
			return errors.New("unknown entry type")
		}
		return nil
	}); err != nil {
		return err
	}

	// Apply committed transactions and clean up
	for txID, state := range txStates {
		switch state {
		case TxnCommitted:
		case TxnAborted:
		case TxnActive:
			log.Printf("unexpected active transaction %d after WAL replay", txID)
		default:
			log.Printf("unknown transaction %d state %d", txID, state)
		}
		// Cleanup finished transactions
		delete(txLogs, txID)
		delete(txStates, txID)
	}
	return nil
}

func (rm *Recovery) Close() error {
	return nil
}

func parseLogEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	err := json.Unmarshal(data, &entry)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}
