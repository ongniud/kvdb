package kvdb

import (
	"encoding/json"
	"errors"
)

// LogType represents the type of log entry
type LogType int

const (
	LogBegin  LogType = 1
	LogUpdate LogType = 2
	LogDelete LogType = 3
	LogCommit LogType = 4
	LogAbort  LogType = 5
)

// LogEntry represents a log entry
type LogEntry struct {
	TxID      uint64  // Transaction ID
	Type      LogType // Log type
	Key       string  // Key for operation
	Value     string  // Value for operation
	Timestamp int64   // Transaction operation time
}

// Decode parses a log entry from a string
func (e *LogEntry) Decode(data []byte) error {
	if e == nil {
		return errors.New("entry is nil")
	}
	if err := json.Unmarshal(data, e); err != nil {
		return err
	}
	return nil
}

func (e *LogEntry) Encode() ([]byte, error) {
	if e == nil {
		return nil, errors.New("entry is nil")
	}
	return json.Marshal(e)
}
