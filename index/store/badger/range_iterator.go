package badger

import "github.com/dgraph-io/badger"

type RangeIterator struct {
	iterator *badger.Iterator
	stop     []byte
}

func (i *RangeIterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *RangeIterator) Next() {
	i.iterator.Next()
}

func (i *RangeIterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *RangeIterator) Key() []byte {
	return i.iterator.Item().Key()
}

func (i *RangeIterator) Value() []byte {
	return i.iterator.Item().Key()
}

func (i *RangeIterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *RangeIterator) Close() error {
	i.iterator.Close()
	return nil
}
