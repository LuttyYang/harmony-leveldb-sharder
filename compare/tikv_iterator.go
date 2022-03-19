package compare

import "sync"

const (
	iteratorOnce = 300
)

type Iterator struct {
	db   *TiKVInstance
	lock sync.Mutex

	limit []byte
	start []byte

	err error
	end bool
	pos int

	keys, values             [][]byte
	currentKey, currentValue []byte
}

func newIterator(db *TiKVInstance, start, limit []byte) *Iterator {
	return &Iterator{
		db:    db,
		start: start,
		limit: limit,
	}
}

func (i *Iterator) Next() bool {
	if i.end {
		return false
	}

	if i.keys == nil {
		if next := i.scanNext(); !next {
			return false
		}
	}

	i.currentKey = i.keys[i.pos]
	i.currentValue = i.values[i.pos]
	i.pos++

	if i.pos >= len(i.keys) {
		i.scanNext()
	}

	return true
}

func (i *Iterator) scanNext() bool {
	keys, values, err := i.db.client.Scan(i.start, i.limit, iteratorOnce)
	if err != nil {
		i.err = err
		i.end = true
		return false
	}

	if len(keys) == 0 {
		i.end = true
		return false
	} else {
		i.start = append(keys[len(keys)-1], 0)
	}

	i.pos = 0
	i.keys = keys
	i.values = values
	return true
}

func (i *Iterator) Error() error {
	return i.err
}

func (i *Iterator) Key() []byte {
	return i.currentKey[len(i.db.prefix):]
}

func (i *Iterator) Value() []byte {
	return i.currentValue
}

func (i *Iterator) Release() {
	i.db = nil
	i.end = true
	i.keys = nil
	i.values = nil
	i.currentKey = nil
	i.currentValue = nil
}
