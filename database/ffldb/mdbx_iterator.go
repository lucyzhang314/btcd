package ffldb

import (
	"github.com/ledgerwatch/erigon-lib/kv"
)

// ldbCacheIter wraps a treap iterator to provide the additional functionality
// needed to satisfy the leveldb iterator.Iterator interface.
type mdbxIterator struct {
	cursor kv.Cursor
}

// Enforce ldbCacheIterator implements the leveldb iterator.Iterator interface.
var _ Iterator = (*mdbxIterator)(nil)

func newMdbxIterator(tx kv.Tx) *mdbxIterator {
	if tx == nil {
		return nil
	}

	csr, err := tx.Cursor(mdbxBucketRoot)
	if err != nil {
		return nil
	}

	csr.First()
	return &mdbxIterator{cursor: csr}
}

// Error is only provided to satisfy the iterator interface as there are no
// errors for this memory-only structure.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *mdbxIterator) Error() error {
	return nil
}

// SetReleaser is only provided to satisfy the iterator interface as there is no
// need to override it.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *mdbxIterator) SetReleaser(releaser Releaser) {
}

// Release is only provided to satisfy the iterator interface.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *mdbxIterator) Release() {
	iter.cursor.Close()
}

func (iter *mdbxIterator) Valid() bool {
	key, _, _ := iter.cursor.Current()
	return len(key) > 0
}

func (iter *mdbxIterator) First() bool {
	_, _, err := iter.cursor.First()
	return err == nil
}

func (iter *mdbxIterator) Last() bool {
	_, _, err := iter.cursor.Last()
	return err == nil
}

func (iter *mdbxIterator) Next() bool {
	_, _, err := iter.cursor.Next()
	return err == nil
}

func (iter *mdbxIterator) Prev() bool {
	_, _, err := iter.cursor.Prev()
	return err == nil
}

func (iter *mdbxIterator) Seek(key []byte) bool {
	_, _, err := iter.cursor.Seek(key)
	return err == nil
}

func (iter *mdbxIterator) Key() []byte {
	ki, _, err := iter.cursor.Current()
	if err != nil {
		return nil
	}
	return ki
}

func (iter *mdbxIterator) Value() []byte {
	_, val, err := iter.cursor.Current()
	if err != nil {
		return nil
	}
	return val
}
