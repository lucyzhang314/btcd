package dumpbtcd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

const (
	bitcoinNet         = wire.MainNet
	dbType             = "ffldb"
	metadataBucketName = "Metadata"
	dumpPath           = "/Users/andy/tmp/btcdDumpData/btcd.bin"
	dbPath             = "/Users/andy/dev/chainData/data.btcd.console.v001/mainnet/blocks_ffldb"
	dbPathRestore      = "/Users/andy/dev/chainData/data.btcd.vscode/mainnet/blocks_ffldb"

	spendjournal = "spendjournal"
	utxosetv2    = "utxosetv2"

	infoStep = 1024 * 102
)

var (
	cfindexparentbucket = []byte("cfindexparentbucket")
	bucketIgnored       = make(map[string]bool)
)

func init() {
	// bucketIgnored[spendjournal] = true
	// bucketIgnored[utxosetv2] = true
}

func StartDump() {
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
		enumeratebucket(dumpWriter, tx, tx.Metadata(), []byte(metadataBucketName), "")
		return nil
	})

	fmt.Println("dump db finished")
}

func enumeratebucket(dumpWriter *bufio.Writer, tx database.Tx, bucket database.Bucket, bucketname []byte, prefix string) {
	if bucket == nil {
		return
	}

	dumpBucket(dumpWriter, bucket, bucketname, prefix)

	bucket.ForEachBucket(func(bucketname []byte) error {
		subBucket := bucket.Bucket(bucketname)
		enumeratebucket(dumpWriter, tx, subBucket, bucketname, prefix+"-")
		return nil
	})
}

func dumpBucket(dumpWriter *bufio.Writer, bucket database.Bucket, bucketname []byte, prefix string) {
	if (bucket == nil) || bucketIgnored[string(bucketname)] {
		return
	}

	totalKey := uint64(0)
	sizek := int64(0)
	sizev := int64(0)

	err := bucket.ForEach(func(k, v []byte) error {
		totalKey++
		return nil
	})
	if (err != nil) || (totalKey < 1) {
		fmt.Printf("bucket %20s no keys, skip to dump it \r\n", prefix+string(bucketname))
		return
	}

	fmt.Printf("start dump bucket %20s , totalKey: %d \r\n", prefix+string(bucketname), totalKey)

	writeBucketInfo(dumpWriter, totalKey, bucketname)

	totalKey = uint64(0)

	bucket.ForEach(func(k, v []byte) error {
		sizek += int64(len(k))
		sizev += int64(len(v))
		totalKey++

		if (totalKey % infoStep) == 0 {
			fmt.Println("dump K/V sequence:", totalKey, string(bucketname))
		}

		if len(k) > 0xEF {
			fmt.Println("key length more than 256, that is:", len(k), string(bucketname))
		}
		if len(v) > 0xEFFFFFFF {
			fmt.Println(" --- value length more than 0xEFFF, that is:", len(v), string(bucketname))
		}

		writeBytesUint16(dumpWriter, k)
		writeBytesUint32(dumpWriter, v)

		return nil
	})
	fmt.Printf("finished dump bucket %20s total %9d sizekey %5d sizev %5d\r\n", prefix+string(bucketname), totalKey, sizek, sizev)
}

var (
	bufTwoBytes  = make([]byte, 2)
	bufFourBytes = make([]byte, 4)
)

func writeBytesUint16(dumpWriter *bufio.Writer, data []byte) {
	writeUint16(dumpWriter, uint16(len(data)))
	dumpWriter.Write(data)
}

func writeBytesUint32(dumpWriter *bufio.Writer, data []byte) {
	writeUint32(dumpWriter, uint32(len(data)))
	dumpWriter.Write(data)
}

func writeUint16(dumpWriter *bufio.Writer, data uint16) {
	binary.LittleEndian.PutUint16(bufTwoBytes, data)
	dumpWriter.Write(bufTwoBytes)
}

func writeUint32(dumpWriter *bufio.Writer, data uint32) {
	binary.LittleEndian.PutUint32(bufFourBytes, data)
	dumpWriter.Write(bufFourBytes)
}

func writeBucketInfo(dumpWriter *bufio.Writer, totalKeyNum uint64, bucketName []byte) {

	buffer64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer64, uint64(totalKeyNum))
	dumpWriter.Write(buffer64)

	writeBytesUint16(dumpWriter, bucketName)
}
