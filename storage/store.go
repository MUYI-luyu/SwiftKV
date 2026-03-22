package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
)

// kvEntry 代表存储中的一个键值对条目
type kvEntry struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
}

// Store 是基于BadgerDB的持久化存储实现
type Store struct {
	db     *badger.DB
	dbPath string
}

// NewStore 创建一个新的Store实例
func NewStore(dbPath string) (*Store, error) {
	// 确保目录存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("创建数据库目录失败: %w", err)
		}
	}

	// 打开BadgerDB数据库
	opts := badger.DefaultOptions(dbPath).
		WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("打开BadgerDB失败: %w", err)
	}

	return &Store{
		db:     db,
		dbPath: dbPath,
	}, nil
}

// Get 从存储中读取一个键的值和版本
func (s *Store) Get(key string) (value string, version uint64, exists bool, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		var entry kvEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}

		value = entry.Value
		version = entry.Version
		exists = true
		return nil
	})

	return
}

// Put 将一个键值对存储到持久化存储中
func (s *Store) Put(key, value string, version uint64) error {
	entry := kvEntry{
		Value:   value,
		Version: version,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Delete 从存储中删除一个键
func (s *Store) Delete(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// GetAll 获取所有键值对（用于加载快照或导出）
func (s *Store) GetAll() (map[string]kvEntry, error) {
	data := make(map[string]kvEntry)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				return err
			}

			data[key] = entry
		}
		return nil
	})

	return data, err
}

// LoadSnapshot 从编码的快照数据中恢复存储
func (s *Store) LoadSnapshot(snapshotData []byte) error {
	var snapshot map[string]kvEntry
	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		return fmt.Errorf("反序列化快照失败: %w", err)
	}

	// 清空现有数据
	if err := s.db.DropAll(); err != nil {
		return fmt.Errorf("清空数据库失败: %w", err)
	}

	// 写入快照数据
	for key, entry := range snapshot {
		if err := s.Put(key, entry.Value, entry.Version); err != nil {
			return err
		}
	}

	return nil
}

// SaveSnapshot 将当前存储导出为快照数据
func (s *Store) SaveSnapshot() ([]byte, error) {
	data, err := s.GetAll()
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

// Close 关闭数据库连接
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// GetStats 获取数据库统计信息
func (s *Store) GetStats() string {
	return "BadgerDB storage"
}

// Clear 清空所有数据（用于测试）
func (s *Store) Clear() error {
	return s.db.DropAll()
}

// DBPath 返回数据库路径
func (s *Store) DBPath() string {
	return s.dbPath
}

// DataSize 返回数据库大小（字节）
func (s *Store) DataSize() (int64, error) {
	var size int64
	err := filepath.Walk(s.dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
