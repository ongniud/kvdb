package main

import (
	"fmt"
)

// DataStore represents the data store
type DataStore struct {
	data map[string]string
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]string),
	}
}

// Apply applies an operation to the data store
func (ds *DataStore) Apply(key, value string) {
	ds.data[key] = value
}

// Rollback rolls back an operation in the data store
func (ds *DataStore) Rollback(key string) {
	delete(ds.data, key)
}

// Print prints the current state of the data store
func (ds *DataStore) Print() {
	fmt.Println("DataStore Contents:")
	for key, value := range ds.data {
		fmt.Printf("%s: %s\n", key, value)
	}
}
