package kvdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDB(t *testing.T) {
	dir := "test_wal"
	//_ = os.RemoveAll(dir)
	//defer os.RemoveAll(dir)

	db, err := NewDB(dir)
	require.NoError(t, err)
	require.NotNil(t, db)

	// 测试 Update 方法（写入数据）
	err = db.Update(func(tx *Transaction) error {
		tx.Put("foo", "bar")
		return nil
	})
	require.NoError(t, err)

	// 测试 Get 方法
	value, found := db.Get("foo")
	require.True(t, found)
	require.Equal(t, "bar", value)

	// 测试 View 方法（只读事务）
	err = db.View(func(tx *Transaction) error {
		v, found := tx.Get("foo")
		require.True(t, found)
		require.Equal(t, "bar", v)
		return nil
	})
	require.NoError(t, err)

	// 测试事务回滚
	err = db.Update(func(tx *Transaction) error {
		if err := tx.Put("baz", "qux"); err != nil {
			fmt.Println(err)
		}
		return assert.AnError // 触发回滚
	})
	require.Error(t, err)

	_, found = db.Get("baz")
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
	require.False(t, found) // 确保数据未提交
}
