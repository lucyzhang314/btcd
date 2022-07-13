package dbx

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

var (
	db    database.DB
	key   = []byte("kvn_key1")
	value = []byte("bityi_value")
)

func TryFfldb() {
	var err error
	dbpath := filepath.Join(os.TempDir(), "testDrvDe_mdbx")
	//dbpath := filepath.Join(os.TempDir(), "testDrvDe_leveldb")
	db, err = database.Create("ffldb", dbpath, wire.MainNet)
	if err != nil {
		fmt.Println("create database error:", err)
		return
	}

	// TestFFldb_two()
	TestFFldb_323()
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

func TestFFldb_323() {
	var err error
	//err = db.Update(func(tx database.Tx) error {
	//	genesisBlock := chaincfg.MainNetParams.GenesisBlock
	//	return tx.StoreBlock(btcutil.NewBlock(genesisBlock))
	//})
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//return

	var loadedBlockBytes []byte
	err = db.View(func(tx database.Tx) error {
		genesisHash := chaincfg.MainNetParams.GenesisHash

		// chekc has block func
		existing, err := tx.HasBlock(genesisHash)
		if err != nil {
			fmt.Println("is block existing:", existing)
			return err
		}

		//blockHashs := []chainhash.Hash{*genesisHash}
		//existings, err := tx.HasBlocks(blockHashs)
		//if err != nil {
		//	fmt.Println("is block existing 2:", exist)
		//	return err
		//}

		// check fetch blocks
		blockBytes, err := tx.FetchBlock(genesisHash)
		if err != nil {
			return err
		}
		//blockBytesMul, err := tx.FetchBlocks(blockHashs)
		//if err != nil {
		//	fmt.Println("block 2:", string(block))
		//	return err
		//}

		// check fetch block header
		headerBytes, err := tx.FetchBlockHeader(genesisHash)
		if err != nil {
			return err
		}
		fmt.Println("header:", string(headerBytes))
		//headerBytesMul, err := tx.FetchBlockHeaders(blockHashs)
		//if err != nil {
		//	fmt.Println("block 2:", string(head))
		//	return err
		//}

		// As documented, all data fetched from the database is only
		// valid during a database transaction in order to support
		// zero-copy backends.  Thus, make a copy of the data so it
		// can be used outside of the transaction.
		loadedBlockBytes = make([]byte, len(blockBytes))
		copy(loadedBlockBytes, blockBytes)

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
