package dumpbtcd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/dbtest/dumpbtcd/lzma"
	"github.com/btcsuite/btcd/wire"
)

const (
	dbType     = "ffldb"
	bitcoinNet = wire.MainNet
	dbPath     = "/Users/andy/dev/chainData/data.btcd.vscode/mainnet/blocks_ffldb"
	dumpPath   = "/Users/andy/tmp/btcdDumpData/btcd.bin"
)

var (
	chainStateKeyName     = []byte("chainstate")
	utxoSetBucketName     = []byte("utxosetv2")
	heightIndexBucketName = []byte("heightidx")
	hashIndexBucketName   = []byte("hashidx")

	// dumpFile    *os.File
	// dumpWriter  *bufio.Writer
)

func StartDump() {

	// compressFile(dumpPath, dumpPath+".compress")
	// decompressFile(dumpPath, dumpPath+".decompress")
	// return

	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}

	// save utxo to file
	dumpFile, err := os.Create(dumpPath)
	if err != nil {
		return
	}
	defer dumpFile.Close()

	dumpWriter := bufio.NewWriter(dumpFile)
	defer dumpWriter.Flush()

	db.View(func(tx database.Tx) error {
		serializedData := tx.Metadata().Get(chainStateKeyName)
		state, err := deserializeBestChainState(serializedData)
		if err != nil {
			return err
		}
		fmt.Printf("height %5d, totalTxns %9d, hash %s, workSum %s\r\n", state.height, state.totalTxns, state.hash, state.workSum.String())

		bufTwoBytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(bufTwoBytes, uint16(len(serializedData)))
		dumpWriter.Write(bufTwoBytes)
		dumpWriter.Write(serializedData)

		// err = tx.Metadata().ForEachBucket(func(k []byte) error {
		// 	fmt.Printf("Bucket: %s \r\n", string(k))
		// 	bkt := tx.Metadata().Bucket(k)
		// 	err = bkt.ForEachBucket(func(k []byte) error {
		// 		fmt.Printf("    Bucket: %s \r\n", string(k))
		// 		return nil
		// 	})
		// 	return nil
		// })

		// enumeratebucket(tx, tx.Metadata(), []byte("rootBucket"), "")

		utxoSetBucket := tx.Metadata().Bucket(utxoSetBucketName)
		dumpBucket(dumpWriter, utxoSetBucket, utxoSetBucketName, "")

		return nil
	})

	fmt.Println("dump db finished")
}

func enumeratebucket(tx database.Tx, bucket database.Bucket, bucketname []byte, prefix string) {
	if bucket == nil {
		return
	}

	// dumpBucket(bucket, bucketname, prefix)

	// inspect k v
	bucket.ForEachBucket(func(bucketname []byte) error {
		subBucket := bucket.Bucket(bucketname)
		enumeratebucket(tx, subBucket, bucketname, prefix+"-")
		return nil
	})

}

func dumpBucket(dumpWriter *bufio.Writer, bucket database.Bucket, bucketname []byte, prefix string) {

	total := 0
	sizek := int64(0)
	sizev := int64(0)
	bufTwoBytes := make([]byte, 2)

	// subBucket := bucket.Bucket(bucketname)
	bucket.ForEach(func(k, v []byte) error {
		sizek += int64(len(k))
		sizev += int64(len(v))
		total++

		dumpWriter.WriteByte(byte(len(k)))
		dumpWriter.Write(k)

		binary.LittleEndian.PutUint16(bufTwoBytes, uint16(len(v)))
		dumpWriter.Write(bufTwoBytes)
		dumpWriter.Write(v)

		return nil
	})
	fmt.Printf("bucket %20s total %9d sizekey %5d sizev %5d\r\n", prefix+string(bucketname), total, sizek, sizev)
}

func compressFile(fileNameIn, fileNameOut string) {

	inputFile, err := os.Open(fileNameIn)
	if err != nil {
		return
	}
	defer inputFile.Close()

	fi, err := inputFile.Stat()
	if err != nil {
		return
	}
	fmt.Println(fi.Size())

	buffer := make([]byte, fi.Size())
	rutxo := bufio.NewReader(inputFile)

	read, err := io.ReadFull(rutxo, buffer)
	fmt.Println("----", read, err)

	// save utxo to file
	compressFile, err := os.Create(fileNameOut)
	if err != nil {
		return
	}
	compressWriter := bufio.NewWriter(compressFile)

	lzmaWriter := lzma.NewWriter(compressWriter)
	lzmaWriter.Write(buffer)
	lzmaWriter.Close()

	fmt.Println("compress finished")
}
