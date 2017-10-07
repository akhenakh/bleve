package badger

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
)

type Writer struct {
	s *Store
	*badger.Txn
}

func (w *Writer) NewBatch() store.KVBatch {
	return &Batch{
		store: w.s,
		merge: store.NewEmulatedMerge(w.s.mo),
		Txn:   w.Txn,
	}
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	// first process merges
	for k, mergeOps := range batch.merge.Merges {
		kb := []byte(k)
		item, err := w.Txn.Get(kb)
		if err != nil {
			return err
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		mergedVal, fullMergeOk := w.s.mo.FullMerge(kb, v, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		w.Txn.Set(kb, mergedVal, 0)
	}

	return w.Txn.Commit(nil)
}

func (w *Writer) Close() error {
	w.s = nil
	return w.Txn.Commit(nil)
}
