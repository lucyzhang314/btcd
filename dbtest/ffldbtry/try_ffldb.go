package ffldbtry

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

var (
	db         database.DB
	key        = []byte("kvn_key1")
	value      = []byte("bityi_value")
	block2Hash chainhash.Hash
	block3Hash chainhash.Hash
	block4Hash chainhash.Hash
)

func TryFfldb() {
	var err error
	dbpath := filepath.Join(os.TempDir(), "testdrvDe_mdbx")
	// dbpath := filepath.Join(os.TempDir(), "testDrvDe_leveldb")
	db, err = database.Create("ffldb", dbpath, wire.MainNet)
	if err != nil {
		fmt.Println("create database error:", err)
		return
	}

	// TestFFldb_bucket()
	TestFFldb_TX()
	//TestFFldb_3()
	//TestFFldb_4()

	//defer os.RemoveAll(dbpath)
	defer db.Close()
}

func TestFFldb_two() {
	// err = db.Update(func(tx database.Tx) error {
	// 	md := tx.Metadata()
	// 	err := md.Put(key, value)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	retrivedValue := md.Get(key)
	// 	if !bytes.Equal(retrivedValue, value) {
	// 		return fmt.Errorf("not equal")
	// 	}

	// 	return nil
	// })
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	db.View(func(tx database.Tx) error {

		readValue := tx.Metadata().Get(key)
		if !bytes.Equal(readValue, value) {
			return fmt.Errorf("unexpected value for key '%s'", key)
		}

		return nil
	})
}

func storeBlocks() {
	var err error
	err = db.Update(func(tx database.Tx) error {
		genesisBlock := chaincfg.MainNetParams.GenesisBlock
		blk := btcutil.NewBlock(genesisBlock)
		blk.SetHeight(0)
		tx.StoreBlock(blk)

		block2 := chaincfg.MainNetParams.GenesisBlock
		block2.Header.PrevBlock = *chaincfg.MainNetParams.GenesisHash
		block2.Header.Timestamp = time.Now()
		block2Hash = block2.BlockHash()
		blk = btcutil.NewBlock(block2)
		blk.SetHeight(1)
		tx.StoreBlock(blk)

		block3 := *genesisBlock
		block3.Header.PrevBlock = block2Hash
		block3.Header.Timestamp = time.Now()
		block3Hash = block3.BlockHash()
		blk = btcutil.NewBlock(&block3)
		blk.SetHeight(2)
		tx.StoreBlock(blk)

		block4 := *genesisBlock
		block4.Header.PrevBlock = block3Hash
		block4.Header.Timestamp = time.Now()
		block4Hash = block4.BlockHash()
		blk = btcutil.NewBlock(&block4)
		blk.SetHeight(3)
		tx.StoreBlock(blk)

		return nil
	})
	fmt.Println(err)
	// if err != nil {
	// 	return
	// }
}

func TestFFldb_TX() {
	var err error
	storeBlocks()

	var loadedBlockBytes []byte
	err = db.View(func(tx database.Tx) error {

		mdd := tx.Metadata()
		fmt.Println(mdd)

		genesisHash := chaincfg.MainNetParams.GenesisHash

		// chekc has block func
		existing, err := tx.HasBlock(genesisHash)
		if err != nil {
			fmt.Println("is block existing:", existing)
			return err
		}

		// check fetch blocks
		blockBytes, err := tx.FetchBlock(genesisHash)
		if err != nil {
			return err
		}

		blockHashs := []chainhash.Hash{*genesisHash, block2Hash, block3Hash, block4Hash}
		existings, err := tx.HasBlocks(blockHashs)
		if err != nil {
			fmt.Println("is block existing 2:", existings)
			return err
		}

		blockBytesMul, err := tx.FetchBlocks(blockHashs)
		if err != nil {
			fmt.Println("block 2:", blockBytesMul)
			return err
		}

		// check fetch block header
		headerBytes, err := tx.FetchBlockHeader(genesisHash)
		if err != nil {
			return err
		}
		fmt.Println("header:", string(headerBytes))

		headerBytesMul, err := tx.FetchBlockHeaders(blockHashs)
		if err != nil {
			fmt.Println("block 2:", headerBytesMul)
			return err
		}

		// As documented, all data fetched from the database is only
		// valid during a database transaction in order to support
		// zero-copy backends.  Thus, make a copy of the data so it
		// can be used outside of the transaction.
		loadedBlockBytes = make([]byte, len(blockBytes))
		copy(loadedBlockBytes, blockBytes)

		data, err := tx.FetchBlockRegion(&database.BlockRegion{
			Hash:   genesisHash,
			Offset: 0,
			Len:    248,
		})
		fmt.Println("header:", data, err)

		data1, err := tx.FetchBlockRegion(&database.BlockRegion{
			Hash:   &block2Hash,
			Offset: 0,
			Len:    248,
		})
		fmt.Println("header:", data1, err)

		ret := bytes.Compare(data, data1)
		fmt.Println("header:", ret)
		// md := tx.Metadata()
		// cursor := md.Cursor()
		// if cursor != nil {
		// 	cursor.First()
		// 	fmt.Println("key:", string(cursor.Key()))
		// 	fmt.Println("value:", string(cursor.Value()))
		// }

		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

func TestFFldb_3() {
	dbPath := filepath.Join(os.TempDir(), "exampleusage")
	db, err := database.Create("ffldb", dbPath, wire.MainNet)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dbPath)
	defer db.Close()

	key := []byte("mykey")
	value := []byte("myvalue")

	err = db.Update(func(tx database.Tx) error {
		if err := tx.Metadata().Put(key, value); err != nil {
			return err
		}

		if !bytes.Equal(tx.Metadata().Get(key), value) {
			return fmt.Errorf("unexpected value for key '%s'", key)
		}

		nestedBucketKey := []byte("mybucket")
		nestedBucket, err := tx.Metadata().CreateBucket(nestedBucketKey)
		if err != nil {
			return err
		}

		// The key from above that was set in the metadata bucket does
		// not exist in this new nested bucket.
		if nestedBucket.Get(key) != nil {
			return fmt.Errorf("key '%s' is not expected nil", key)
		}

		return nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	err = db.View(func(tx database.Tx) error {

		if !bytes.Equal(tx.Metadata().Get(key), value) {
			return fmt.Errorf("unexpected value for key '%s'", key)
		}

		return nil
	})

}

func TestFFldb_4() {
	dbPath := filepath.Join(os.TempDir(), "dbTestingTmp_4")
	db, err := database.Create("ffldb", dbPath, wire.MainNet)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dbPath)
	defer db.Close()

	key := []byte("mykey")
	value := []byte("myvalue")

	tx, err := db.Begin(true)
	if err != nil {
		fmt.Println(err)
		return
	}

	if err := tx.Metadata().Put(key, value); err != nil {
		fmt.Println(err)
		_ = tx.Rollback()
		return
	}

	err = tx.Commit()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = db.View(func(tx database.Tx) error {

		if !bytes.Equal(tx.Metadata().Get(key), value) {
			return fmt.Errorf("unexpected value for key '%s'", key)
		}

		return nil
	})

}

func TestFFldb_bucket() {
	// var rootBucket database.Bucket
	// err := db.View(func(tx database.Tx) error {
	// 	rootBucket = tx.Metadata()
	// 	return nil
	// })
	// if err != nil || rootBucket == nil {
	// 	return
	// }

	// nestedBucketKey := []byte("mybucket")
	// nestedBucket, err := rootBucket.CreateBucket(nestedBucketKey)
	// if err != nil {
	// 	return
	// }

	// // The key from above that was set in the metadata bucket does
	// // not exist in this new nested bucket.
	// val := nestedBucket.Get(key)
	// if val != nil {
	// 	fmt.Errorf("key '%s' is not expected nil", key)
	// }

	err := db.Update(func(tx database.Tx) error {

		rootBucket := tx.Metadata()

		// if err := tx.Metadata().Put(key, value); err != nil {
		// 	return err
		// }

		// if !bytes.Equal(tx.Metadata().Get(key), value) {
		// 	return fmt.Errorf("unexpected value for key '%s'", key)
		// }

		nestedBucketKey := []byte("mybucket2")
		nestedBucket, err := rootBucket.CreateBucket(nestedBucketKey)
		if err != nil {
			return err
		}

		// The key from above that was set in the metadata bucket does
		// not exist in this new nested bucket.
		if nestedBucket.Get(key) != nil {
			return fmt.Errorf("key '%s' is not expected nil", key)
		}

		return nil
	})
	fmt.Println(err)
}
