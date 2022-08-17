package dumpmdbx

import (
	"fmt"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

const (
	bitcoinNet         = wire.MainNet
	dbType             = "ffldb"
	metadataBucketName = "Metadata"
)

var (
	blockheaderidx = []byte("blockheaderidx")
	hashidx        = []byte("hashidx")
	heightidx      = []byte("heightidx")
	spendjournal   = []byte("spendjournal")
	utxosetv2      = []byte("utxosetv2")
	idxtips        = []byte("idxtips")

	cfindexparentbucket = []byte("cfindexparentbucket")
	cf0byhashidx        = []byte("cf0byhashidx")
	cf0headerbyhashidx  = []byte("cf0headerbyhashidx")
	cf0hashbyhashidx    = []byte("cf0hashbyhashidx")
)

// prune ffldb bucket before dump MDBX database
func pruneBucket(dbPath string) {
	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}
	defer db.Close()

	db.Update(func(tx database.Tx) error {
		tx.Metadata().Bucket(cfindexparentbucket).DeleteBucket(cf0byhashidx)
		tx.Metadata().Bucket(cfindexparentbucket).DeleteBucket(cf0hashbyhashidx)
		tx.Metadata().DeleteBucket(hashidx)
		tx.Metadata().DeleteBucket(heightidx)
		tx.Metadata().DeleteBucket(spendjournal)
		return nil
	})

	fmt.Println("Prune db finished")
}

// create ffldb bucket before restore MDBX database
func createBuckets(dbPath string) {

	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}
	defer db.Close()

	db.Update(func(tx database.Tx) error {

		metaBucket := tx.Metadata()

		//
		// WARNING:
		// create bucket sequence is very critical, DON'T change them !
		// It's MUST be the same with BTCD
		//
		metaBucket.CreateBucketIfNotExists(blockheaderidx)
		metaBucket.CreateBucketIfNotExists(hashidx)
		metaBucket.CreateBucketIfNotExists(heightidx)
		metaBucket.CreateBucketIfNotExists(spendjournal)
		metaBucket.CreateBucketIfNotExists(utxosetv2)
		metaBucket.CreateBucketIfNotExists(idxtips)

		metaBucket.CreateBucketIfNotExists(cfindexparentbucket)
		metaBucket.Bucket(cfindexparentbucket).CreateBucketIfNotExists(cf0byhashidx)
		metaBucket.Bucket(cfindexparentbucket).CreateBucketIfNotExists(cf0headerbyhashidx)
		metaBucket.Bucket(cfindexparentbucket).CreateBucketIfNotExists(cf0hashbyhashidx)

		return nil
	})
}

func DisplayBucketInfo(dbPath string) {
	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}
	defer db.Close()

	db.View(func(tx database.Tx) error {
		enumeratebucket(tx, tx.Metadata(), []byte(metadataBucketName), "")
		return nil
	})

	fmt.Println("dump db finished")
}

func enumeratebucket(tx database.Tx, bucket database.Bucket, bucketname []byte, prefix string) {
	if bucket == nil {
		return
	}

	displayBucket(bucket, bucketname, prefix)

	bucket.ForEachBucket(func(bucketname []byte) error {
		subBucket := bucket.Bucket(bucketname)
		enumeratebucket(tx, subBucket, bucketname, prefix+"-")
		return nil
	})
}

func displayBucket(bucket database.Bucket, bucketname []byte, prefix string) {
	if bucket == nil {
		return
	}

	totalKey := uint64(0)
	sizek := int64(0)
	sizev := int64(0)

	// fmt.Printf("start dump bucket %20s , totalKey: %d \r\n", prefix+string(bucketname), totalKey)

	totalKey = uint64(0)

	bucket.ForEach(func(k, v []byte) error {
		sizek += int64(len(k))
		sizev += int64(len(v))
		totalKey++

		return nil
	})

	// fmt.Println(string(bucketname))

	const divisor = 1024 * 1024 // 1M
	fmt.Printf("bucketName:%24s totalItems:%9d sizeKey:%5dM sizeValue:%5dM\r\n", prefix+string(bucketname), totalKey, sizek/divisor, sizev/divisor)
	// fmt.Printf("bucketName:%24s bucketID:%d\r\n", prefix+string(bucketname), bucket.Info())
}
