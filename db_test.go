package kvdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	dir := "test_wal"
	//_ = os.RemoveAll(dir)
	//defer os.RemoveAll(dir)

	db, err := NewDB(dir)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Update(func(tx *Transaction) error {
		tx.Put("foo", "bar")
		return nil
	})
	require.NoError(t, err)

	err = db.View(func(tx *Transaction) error {
		v, found := tx.Get("foo")
		require.True(t, found)
		require.Equal(t, "bar", v)
		return nil
	})
	require.NoError(t, err)

	err = db.Update(func(tx *Transaction) error {
		if err := tx.Put("baz", "qux"); err != nil {
			fmt.Println(err)
		}
		return assert.AnError
	})
	require.Error(t, err)

	db.Update(func(tx *Transaction) error {
		return nil
	})
	db.Update(func(tx *Transaction) error {
		return nil
	})
	db.Update(func(tx *Transaction) error {
		return nil
	})
	db.Update(func(tx *Transaction) error {
		return nil
	})
	db.Update(func(tx *Transaction) error {
		return nil
	})
}
