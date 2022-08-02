// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"bytes"
	"encoding/binary"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire"
)

const (
	// metadataDbName is the name used for the metadata database.
	metadataDbName = "metadata"

	// blockHdrSize is the size of a block header.  This is simply the
	// constant from wire and is only provided here for convenience since
	// wire.MaxBlockHeaderPayload is quite long.
	blockHdrSize = wire.MaxBlockHeaderPayload

	// blockHdrOffset defines the offsets into a block index row for the
	// block header.
	//
	// The serialized block index row format is:
	//   <blocklocation><blockheader>
	blockHdrOffset = blockLocSize
)

var (
	// byteOrder is the preferred byte order used through the database and
	// block files.  Sometimes big endian will be used to allow ordered byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	bucketIndexPrefix = []byte("bidx")

	// curBucketIDKeyName is the name of the key used to keep track of the
	// current bucket ID counter.
	curBucketIDKeyName = []byte("bidx-cbid")

	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32.
	metadataBucketID = [4]byte{}

	// blockIdxBucketID is the ID of the internal block metadata bucket.
	// It is the value 1 encoded as an unsigned big-endian uint32.
	blockIdxBucketID = [4]byte{0x00, 0x00, 0x00, 0x01}

	// blockIdxBucketName is the bucket used internally to track block
	// metadata.
	blockIdxBucketName = []byte("ffldb-blockidx")

	// writeLocKeyName is the key used to store the current write file
	// location.
	writeLocKeyName = []byte("ffldb-writeloc")
)

// Common error strings.
const (
	// errDbNotOpenStr is the text to use for the database.ErrDbNotOpen
	// error code.
	errDbNotOpenStr = "database is not open"

	// errTxClosedStr is the text to use for the database.ErrTxClosed error
	// code.
	errTxClosedStr = "database tx is closed"
)

// makeDbErr creates a database.Error given a set of arguments.
func makeDbErr(c database.ErrorCode, desc string, err error) database.Error {
	return database.Error{ErrorCode: c, Description: desc, Err: err}
}

// convertErr converts the passed leveldb error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func convertErr(desc string, ldbErr error) database.Error {
	// Use the driver-specific error code by default.  The code below will
	// update this with the converted error if it's recognized.
	var code = database.ErrDriverSpecific

	switch {
	// Database corruption errors.
	// case ldberrors.IsCorrupted(ldbErr):
	// 	code = database.ErrCorruption

	// Database open/create errors.
	// case ldbErr == leveldb.ErrClosed:
	// 	code = database.ErrDbNotOpen

	// // Transaction errors.
	// case ldbErr == leveldb.ErrSnapshotReleased:
	// 	code = database.ErrTxClosed
	// case ldbErr == leveldb.ErrIterReleased:
	// 	code = database.ErrTxClosed
	}

	return database.Error{ErrorCode: code, Description: desc, Err: ldbErr}
}

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// leveldb iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	if len(slice) < 1 {
		return nil
	}

	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the database.Cursor interface.
type cursor struct {
	bucket      *bucket
	dbIter      Iterator
	pendingIter Iterator
	currentIter Iterator
}

// Enforce cursor implements the database.Cursor interface.
var _ database.Cursor = (*cursor)(nil)

// Bucket returns the bucket the cursor was created for.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Bucket() database.Bucket {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	return c.bucket
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
//
// Returns the following errors as required by the interface contract:
//   - ErrIncompatibleValue if attempted when the cursor points to a nested
//     bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Delete() error {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}

	// Error if the cursor is exhausted.
	if c.currentIter == nil {
		str := "cursor is exhausted"
		return makeDbErr(database.ErrIncompatibleValue, str, nil)
	}

	// Do not allow buckets to be deleted via the cursor.
	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		str := "buckets may not be deleted from a cursor"
		return makeDbErr(database.ErrIncompatibleValue, str, nil)
	}

	c.bucket.tx.deleteKey(copySlice(key), true)
	return nil
}

// skipPendingUpdates skips any keys at the current database iterator position
// that are being updated by the transaction.  The forwards flag indicates the
// direction the cursor is moving.
func (c *cursor) skipPendingUpdates(forwards bool) {
	for c.dbIter.Valid() {
		var skip bool
		key := c.dbIter.Key()
		if c.bucket.tx.pendingRemove.Has(key) {
			skip = true
		} else if c.bucket.tx.pendingKeys.Has(key) {
			skip = true
		}
		if !skip {
			break
		}

		if forwards {
			c.dbIter.Next()
		} else {
			c.dbIter.Prev()
		}
	}
}

// chooseIterator first skips any entries in the database iterator that are
// being updated by the transaction and sets the current iterator to the
// appropriate iterator depending on their validity and the order they compare
// in while taking into account the direction flag.  When the cursor is being
// moved forwards and both iterators are valid, the iterator with the smaller
// key is chosen and vice versa when the cursor is being moved backwards.
func (c *cursor) chooseIterator(forwards bool) bool {
	// Skip any keys at the current database iterator position that are
	// being updated by the transaction.
	c.skipPendingUpdates(forwards)

	// When both iterators are exhausted, the cursor is exhausted too.
	if !c.dbIter.Valid() && !c.pendingIter.Valid() {
		c.currentIter = nil
		return false
	}

	// Choose the database iterator when the pending keys iterator is
	// exhausted.
	if !c.pendingIter.Valid() {
		c.currentIter = c.dbIter
		return true
	}

	// Choose the pending keys iterator when the database iterator is
	// exhausted.
	if !c.dbIter.Valid() {
		c.currentIter = c.pendingIter
		return true
	}

	// Both iterators are valid, so choose the iterator with either the
	// smaller or larger key depending on the forwards flag.
	compare := bytes.Compare(c.dbIter.Key(), c.pendingIter.Key())
	if (forwards && compare > 0) || (!forwards && compare < 0) {
		c.currentIter = c.pendingIter
	} else {
		c.currentIter = c.dbIter
	}
	return true
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) First() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the first key in both the database and pending iterators and
	// choose the iterator that is both valid and has the smaller key.
	c.dbIter.First()
	c.pendingIter.First()
	return c.chooseIterator(true)
}

// Last positions the cursor at the last key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Last() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the last key in both the database and pending iterators and
	// choose the iterator that is both valid and has the larger key.
	c.dbIter.Last()
	c.pendingIter.Last()
	return c.chooseIterator(false)
}

// Next moves the cursor one key/value pair forward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Next() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return false
	}

	// Move the current iterator to the next entry and choose the iterator
	// that is both valid and has the smaller key.
	c.currentIter.Next()
	return c.chooseIterator(true)
}

// Prev moves the cursor one key/value pair backward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Prev() bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return false
	}

	// Move the current iterator to the previous entry and choose the
	// iterator that is both valid and has the larger key.
	c.currentIter.Prev()
	return c.chooseIterator(false)
}

// Seek positions the cursor at the first key/value pair that is greater than or
// equal to the passed seek key.  Returns false if no suitable key was found.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) bool {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	// Seek to the provided key in both the database and pending iterators
	// then choose the iterator that is both valid and has the larger key.
	seekKey := bucketizedKey(c.bucket.id, seek)
	c.dbIter.Seek(seekKey)
	c.pendingIter.Seek(seekKey)
	return c.chooseIterator(true)
}

// rawKey returns the current key the cursor is pointing to without stripping
// the current bucket prefix or bucket index prefix.
func (c *cursor) rawKey() []byte {
	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	return copySlice(c.currentIter.Key())
}

// Key returns the current key the cursor is pointing to.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Key() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	// Slice out the actual key name and make a copy since it is no longer
	// valid after iterating to the next item.
	//
	// The key is after the bucket index prefix and parent ID when the
	// cursor is pointing to a nested bucket.
	key := c.currentIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		key = key[len(bucketIndexPrefix)+4:]
		return copySlice(key)
	}

	// The key is after the bucket ID when the cursor is pointing to a
	// normal entry.
	key = key[len(c.bucket.id):]
	return copySlice(key)
}

// rawValue returns the current value the cursor is pointing to without
// stripping without filtering bucket index values.
func (c *cursor) rawValue() []byte {
	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	return copySlice(c.currentIter.Value())
}

// Value returns the current value the cursor is pointing to.  This will be nil
// for nested buckets.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Value() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if cursor is exhausted.
	if c.currentIter == nil {
		return nil
	}

	// Return nil for the value when the cursor is pointing to a nested
	// bucket.
	if bytes.HasPrefix(c.currentIter.Key(), bucketIndexPrefix) {
		return nil
	}

	return copySlice(c.currentIter.Value())
}

// cursorType defines the type of cursor to create.
type cursorType int

// The following constants define the allowed cursor types.
const (
	// ctKeys iterates through all of the keys in a given bucket.
	ctKeys cursorType = iota

	// ctBuckets iterates through all directly nested buckets in a given
	// bucket.
	ctBuckets

	// ctFull iterates through both the keys and the directly nested buckets
	// in a given bucket.
	ctFull
)

// cursorFinalizer is either invoked when a cursor is being garbage collected or
// called manually to ensure the underlying cursor iterators are released.
func cursorFinalizer(c *cursor) {
	c.dbIter.Release()
	c.pendingIter.Release()
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType) *cursor {
	var dbIter, pendingIter Iterator
	switch cursorTyp {
	case ctKeys:
		keyRange := BytesPrefix(bucketID)
		dbIter = b.tx.snapshot.NewIterator(keyRange, b.tx)
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingIter = pendingKeyIter

	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>

		// Create an iterator for the both the database and the pending
		// keys which are prefixed by the bucket index identifier and
		// the provided bucket ID.
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := BytesPrefix(prefix)

		dbIter = b.tx.snapshot.NewIterator(bucketRange, b.tx)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		pendingIter = pendingBucketIter

	case ctFull:
		fallthrough
	default:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := BytesPrefix(prefix)
		keyRange := BytesPrefix(bucketID)

		// Since both keys and buckets are needed from the database,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		dbKeyIter := b.tx.snapshot.NewIterator(keyRange, b.tx)
		dbBucketIter := b.tx.snapshot.NewIterator(bucketRange, b.tx)
		iters := []Iterator{dbKeyIter, dbBucketIter}
		dbIter = NewMergedIterator(iters, DefaultComparer, true)

		// Since both keys and buckets are needed from the pending keys,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		pendingKeyIter := newLdbTreapIter(b.tx, keyRange)
		pendingBucketIter := newLdbTreapIter(b.tx, bucketRange)
		iters = []Iterator{pendingKeyIter, pendingBucketIter}
		pendingIter = NewMergedIterator(iters, DefaultComparer, true)
	}

	// Create the cursor using the iterators.
	return &cursor{bucket: b, dbIter: dbIter, pendingIter: pendingIter}
}
