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
	block2Hash chainhash.Hash
	block3Hash chainhash.Hash
	block4Hash chainhash.Hash
	// key        = []byte("kvn_key1")
	// value      = []byte("bityi_value")
	numberErrors int
)

func TryFfldb() {
	var err error
	dbpath := filepath.Join(os.TempDir(), "testdrvDe_mdbx")
	// dbpath := filepath.Join(os.TempDir(), "testdrvDe_leveldb")
	db, err = database.Create("ffldb", dbpath, wire.MainNet)
	if err != nil {
		fmt.Println("create database error:", err)
		return
	}
	fmt.Println(db.Type())

	blockHeigh := int32(0)
	steps := 10000

	for blockHeigh < int32(steps) { // *1000*10000
		fmt.Println("index:", blockHeigh)
		// storeBlocks_one(blockHeigh, steps)
		storeBlocks_two(blockHeigh, steps)
		blockHeigh += int32(steps)
	}

	// readBlocks()

	//defer os.RemoveAll(dbpath)
	defer db.Close()

	fmt.Println("-------------- end -------------------, total error numbers:", numberErrors)
}

func storeBlocks_one(initHeight int32, steps int) {
	tx, err := db.Begin(true)
	if err != nil {
		return
	}

	add_blocks_to_tx(tx, initHeight, steps)

	if err != nil {
		tx.Rollback()
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
	}
}

func storeBlocks_two(initHeight int32, steps int) {
	err := db.Update(func(tx database.Tx) error {
		add_blocks_to_tx(tx, initHeight, steps)
		return nil
	})
	fmt.Println(err)
}

func readBlocks() {
	err := db.View(func(tx database.Tx) error {

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
		fmt.Println("is block existing:", blockBytes)

		headerBytes, err := tx.FetchBlockHeader(genesisHash)
		if err != nil {
			return err
		}
		fmt.Println("is block existing:", headerBytes)

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
		headerBytes, err = tx.FetchBlockHeader(genesisHash)
		if err != nil {
			return err
		}
		fmt.Println("header:", string(headerBytes))

		headerBytesMul, err := tx.FetchBlockHeaders(blockHashs)
		if err != nil {
			fmt.Println("block 2:", headerBytesMul)
			return err
		}

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

		return nil
	})

	if err != nil {
		fmt.Println(err)
	}
}

func add_blocks_to_tx(tx database.Tx, initHeight int32, steps int) {
	// genesisBlock := chaincfg.MainNetParams.GenesisBlock
	// blk := btcutil.NewBlock(genesisBlock)
	// blk.SetHeight(initHeight)
	// err := tx.StoreBlock(blk)
	// if err != nil {
	// 	fmt.Println(err)
	// 	numberErrors++
	// }

	// initHeight++
	// block2 := chaincfg.MainNetParams.GenesisBlock
	// block2.Header.PrevBlock = *chaincfg.MainNetParams.GenesisHash
	// block2.Header.Timestamp = time.Now()
	// blk = btcutil.NewBlock(block2)
	// blk.SetHeight(initHeight)
	// block2Hash = block2.BlockHash()
	// tx.StoreBlock(blk)

	// initHeight++
	// block3 := *genesisBlock
	// block3.Header.PrevBlock = block2Hash
	// block3.Header.Timestamp = time.Now()
	// blk = btcutil.NewBlock(&block3)
	// blk.SetHeight(initHeight)
	// block3Hash = block3.BlockHash()
	// tx.StoreBlock(blk)

	// initHeight++
	// block4 := *genesisBlock
	// block4.Header.PrevBlock = block3Hash
	// block4.Header.Timestamp = time.Now()
	// blk = btcutil.NewBlock(&block4)
	// blk.SetHeight(initHeight)
	// block4Hash = block4.BlockHash()
	// tx.StoreBlock(blk)

	initHeight++
	blockHash := *chaincfg.MainNetParams.GenesisHash
	for num := 0; num < steps; num++ {
		block4 := *chaincfg.MainNetParams.GenesisBlock
		block4.Header.PrevBlock = blockHash
		block4.Header.Timestamp = time.Now()
		block4.Header.Nonce = uint32(initHeight + int32(num))

		for i := 1; i < 2; i++ {
			block4.Transactions = append(block4.Transactions, build_one_bitcoin_tx(i))
		}

		blk := btcutil.NewBlock(&block4)
		blk.SetHeight(initHeight + int32(num))
		blockHash = block4.BlockHash()
		err := tx.StoreBlock(blk)
		if err != nil {
			fmt.Println(err)
			numberErrors++
		}
	}
}

func build_one_bitcoin_tx(ver int) *wire.MsgTx {
	onetx := wire.NewMsgTx(int32(ver))
	tmp1 := [chainhash.HashSize]byte{} //(string("e789a63091b276e0b39e712711668285"))
	copy(tmp1[:], []byte(string("e789a63091b276e0b39e712711668285")))
	sign := []byte("828504190db")
	witeness := [][]byte{
		[]byte(string("828504190dbede76cd4")),
		// []byte(string("528504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("728504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("888504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("829504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828404190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828524190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828505190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504f90dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("8285041f0dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("82850419xdbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190vbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190dw2ede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190dbe4e76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190dbedb76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190dbede26cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
		// []byte(string("828504190dbede7gcd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461aeeec356de62759828504190dbede76cd461aeeec356de62759e789a63091b276e0b39e71271166828504190dbede76cd461a")),
	}
	onetx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash(tmp1), Index: uint32(1)}, sign, witeness))

	return onetx
}

// func TestFFldb_bucket() {
// 	var rootBucket database.Bucket
// 	err := db.Update(func(tx database.Tx) error {
// 		rootBucket = tx.Metadata()
// 		csr := rootBucket.Cursor()

// 		success := csr.First()
// 		fmt.Println(success)

// 		// ki := csr.Key()
// 		// val := csr.Value()
// 		// fmt.Println(string(ki), string(val))

// 		rootBucket.ForEach(func(k, v []byte) error {
// 			fmt.Println(string(k), string(v))
// 			return nil
// 		})
// 		return nil
// 	})
// 	if err != nil || rootBucket == nil {
// 		return
// 	}

// 	// nestedBucketKey := []byte("mybucket")
// 	// nestedBucket, err := rootBucket.CreateBucket(nestedBucketKey)
// 	// if err != nil {
// 	// 	return
// 	// }

// 	// // The key from above that was set in the metadata bucket does
// 	// // not exist in this new nested bucket.
// 	// val := nestedBucket.Get(key)
// 	// if val != nil {
// 	// 	fmt.Errorf("key '%s' is not expected nil", key)
// 	// }

// 	err = db.Update(func(tx database.Tx) error {

// 		rootBucket = tx.Metadata()

// 		// rootBucket.Cursor()

// 		if err := rootBucket.Put(key, value); err != nil {
// 			return err
// 		}

// 		for idx := 0; idx < 3; idx++ {
// 			keyi := []byte(fmt.Sprintf("key_%d", idx))
// 			vali := []byte(fmt.Sprintf("value_%d", idx))
// 			err := rootBucket.Put(keyi, vali)
// 			fmt.Println(err)
// 		}

// 		// if !bytes.Equal(tx.Metadata().Get(key), value) {
// 		// 	return fmt.Errorf("unexpected value for key '%s'", key)
// 		// }

// 		// nestedBucketKey := []byte("mybucket2")
// 		// nestedBucket, err := rootBucket.CreateBucket(nestedBucketKey)
// 		// if err != nil {
// 		// 	return err
// 		// }

// 		// // The key from above that was set in the metadata bucket does
// 		// // not exist in this new nested bucket.
// 		// if nestedBucket.Get(key) != nil {
// 		// 	return fmt.Errorf("key '%s' is not expected nil", key)
// 		// }

// 		return nil
// 	})
// 	fmt.Println(err)
// }
