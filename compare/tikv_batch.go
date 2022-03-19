package compare

import (
	"errors"
	"sync"
)

var ErrEmptyKeyOrValue = errors.New("empty key or value is not supported")

type Batch struct {
	db   *TiKVInstance
	lock sync.Mutex

	size            int
	batchWriteKey   [][]byte
	batchWriteValue [][]byte
	batchDeleteKey  [][]byte
}

func newBatch(db *TiKVInstance) *Batch {
	return &Batch{db: db}
}

func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return ErrEmptyKeyOrValue
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	b.batchWriteKey = append(b.batchWriteKey, append(b.db.prefix, key...))
	b.batchWriteValue = append(b.batchWriteValue, value)
	b.size += len(b.db.prefix) + len(key) + len(value)
	return nil
}

func (b *Batch) Delete(key []byte) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.batchDeleteKey = append(b.batchDeleteKey, append(b.db.prefix, key...))
	b.size += len(b.db.prefix) + len(key)
}

func (b *Batch) ValueSize() int {
	return b.size
}

func (b *Batch) Write() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.batchWriteKey) > 0 {
		err := b.db.client.BatchPut(b.batchWriteKey, b.batchWriteValue)
		if err != nil {
			return err
		}
	}

	if len(b.batchDeleteKey) > 0 {
		err := b.db.client.BatchDelete(b.batchDeleteKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *Batch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.batchWriteKey = b.batchWriteKey[:0]
	b.batchWriteValue = b.batchWriteValue[:0]
	b.batchDeleteKey = b.batchDeleteKey[:0]
	b.size = 0
}

func (b *Batch) Len() int {
	return b.size
}
