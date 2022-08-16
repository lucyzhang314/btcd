package ffldbtry

import (
	"fmt"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

const (
	bitcoinNet         = wire.MainNet
	metadataBucketName = "Metadata"
	dbType             = "ffldb"
	dbPath             = "/Users/andy/dev/chainData/data.btcd.test/mainnet/blocks_ffldb"
)

var (
	blockheaderidx      = []byte("blockheaderidx")
	cfindexparentbucket = []byte("cfindexparentbucket")
	cf0byhashidx        = []byte("cf0byhashidx")
	cf0hashbyhashidx    = []byte("cf0hashbyhashidx")
	cf0headerbyhashidx  = []byte("cf0headerbyhashidx")
	ffldbblockidx       = []byte("ffldb-blockidx")
	hashidx             = []byte("hashidx")
	heightidx           = []byte("heightidx")
	idxtips             = []byte("idxtips")
	spendjournal        = []byte("spendjournal")
	utxosetv2           = []byte("utxosetv2")
)

func PruneBucket() {
	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}
	defer db.Close()

	db.Update(func(tx database.Tx) error {
		// tx.Metadata().DeleteBucket(blockheaderidx)

		// tx.Metadata().DeleteBucket(cfindexparentbucket)
		tx.Metadata().Bucket(cfindexparentbucket).DeleteBucket(cf0byhashidx)
		// tx.Metadata().Bucket(cfindexparentbucket).DeleteBucket(cf0hashbyhashidx)
		// tx.Metadata().Bucket(cfindexparentbucket).DeleteBucket(cf0headerbyhashidx)

		// tx.Metadata().DeleteBucket(ffldbblockidx)
		// tx.Metadata().DeleteBucket(hashidx)
		// tx.Metadata().DeleteBucket(heightidx)
		// tx.Metadata().DeleteBucket(idxtips)
		// tx.Metadata().DeleteBucket(spendjournal)
		// tx.Metadata().DeleteBucket(utxosetv2)
		return nil
	})

	fmt.Println("dump db finished")
}

func DisplayBucketInfo() {
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

	fmt.Printf("bucketName:%20s totalItems:%9d sizeKey:%5dM sizeValue:%5dM\r\n", prefix+string(bucketname), totalKey, sizek/1024, sizev/1024)
}
