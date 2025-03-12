package main

import (
	"encoding/json"
	"fmt"
)

// LogType represents the type of log entry
type LogType int

const (
	LogBegin  LogType = 1
	LogUpdate LogType = 2
	LogCommit LogType = 3
	LogAbort  LogType = 4
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

func main() {
	// 创建数据存储
	dataStore := NewDataStore()

	// 创建恢复管理器
	recoveryManager, err := NewRecoveryManager("wal.log", dataStore)
	if err != nil {
		fmt.Println("Failed to create recovery manager:", err)
		return
	}
	defer recoveryManager.Close()

	// 执行恢复
	err = recoveryManager.Recover()
	if err != nil {
		fmt.Println("Recovery failed:", err)
		return
	}

	// 打印恢复后的数据
	dataStore.Print()
}
