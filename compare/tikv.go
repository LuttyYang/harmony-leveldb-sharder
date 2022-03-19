package compare

import (
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"sync"
)

type TiKVInstance struct {
	lock         sync.Mutex
	client       *tikv.RawKVClient
	prefix       []byte
	currentBatch *Batch
}

func NewTiKVInstance(pdAddr []string, prefix []byte) (*TiKVInstance, error) {
	client, err := tikv.NewRawKVClient(pdAddr, config.DefaultConfig().Security)
	if err != nil {
		return nil, err
	}

	ins := &TiKVInstance{client: client, prefix: prefix}
	ins.currentBatch = newBatch(ins)

	return ins, nil
}

func (t *TiKVInstance) NewIterator(slice *util.Range) DBIterator {
	return newIterator(t, slice.Start, slice.Limit)
}

func (t *TiKVInstance) Delete(key []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.currentBatch.Delete(key)
	if t.currentBatch.Len() > 1*1024*1024 {
		return t.writeBatch()
	}
	return nil
}

func (t *TiKVInstance) Add(key, val []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	err := t.currentBatch.Put(key, val)
	if err != nil {
		return err
	}

	if t.currentBatch.Len() > 1*1024*1024 {
		return t.writeBatch()
	}
	return nil
}

func (t *TiKVInstance) Flush() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.writeBatch()
}

func (t *TiKVInstance) writeBatch() error {
	err := t.currentBatch.Write()
	if err != nil {
		return err
	}

	t.currentBatch.Reset()
	return nil
}

func (t *TiKVInstance) Close() error {
	err := t.Flush()
	if err != nil {
		return err
	}

	return t.client.Close()
}
