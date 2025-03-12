package kvdb

import (
	"encoding/json"
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

// ParseLogEntry parses a log entry from a string
func (le *LogEntry) ParseLogEntry(line string) error {
	err := json.Unmarshal([]byte(line), le)
	if err != nil {
		return err
	}
	return nil
}
