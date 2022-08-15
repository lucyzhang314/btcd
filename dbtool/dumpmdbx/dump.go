package dumpmdbx

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"time"

	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	mdbxlog "github.com/ledgerwatch/log/v3"
)

const (
	mdbxBucketRoot     = "bucketRoot"
	compressedFilename = "mdbx.dump"
	blockFileSuffix    = ".fdb"
	infoStep           = 1024 * 1024
)

var (
	bufTwoBytes  = make([]byte, 2)
	bufFourBytes = make([]byte, 4)
)

func StartDump(dbPathDump, dumpFileDir string) {
	tmpFilename := path.Join(os.TempDir(), fmt.Sprintf("btcd%d.bin", time.Now().Unix()))
	defer os.Remove(tmpFilename)
	blockFileDir := path.Join(dbPathDump, subBlockFileDir)

	// dump database
	dumpDB(blockFileDir, tmpFilename)

	// make sure target dir existing
	mkdir(dumpFileDir)
	compressFile(tmpFilename, path.Join(dumpFileDir, compressedFilename))

	// compress last block file
	lastBlockFile, err := getLastBlockFile(blockFileDir, blockFileSuffix)
	if err != nil {
		fmt.Println("DUMP db failed because get last block file failed in: ", blockFileDir)
	}
	compressFile(path.Join(blockFileDir, lastBlockFile), path.Join(dumpFileDir, lastBlockFile))

	fmt.Println("Congratulations, dump db completed")
}

func dumpDB(dbPathDump, tmpFilePath string) {
	logger := mdbxlog.New()
	dbpath := path.Join(dbPathDump, metadataDir)
	if !fileExists(dbpath) {
		fmt.Printf("source DB dir: %s not existing.\n", dbpath)
		return
	}

	mdb := mdbx.NewMDBX(logger).Path(dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			mdbxBucketRoot: kv.TableCfgItem{Flags: kv.Default},
		}
	}).MustOpen()
	if mdb == nil {
		fmt.Println("open database failed")
		return
	}
	defer mdb.Close()

	dumpFile, err := os.Create(tmpFilePath)
	if err != nil {
		fmt.Println("open target file failed:", err)
		return
	}
	defer dumpFile.Close()

	dumpWriter := bufio.NewWriter(dumpFile)
	defer dumpWriter.Flush()

	fmt.Println("Starting to dump db at:", dbpath, " sequence: 0")
	totalItems := uint64(0)
	ctx := context.Background()
	mdb.View(ctx, func(tx kv.Tx) error {
		cursor, _ := tx.Cursor(mdbxBucketRoot)
		defer cursor.Close()

		cursor.First()
		for key, val, _ := cursor.Current(); key != nil; key, val, _ = cursor.Next() {
			writeBytesUint16(dumpWriter, key)
			writeBytesUint32(dumpWriter, val)
			totalItems++
			if (totalItems % infoStep) == 0 {
				fmt.Println("dump DB sequence:", totalItems)
			}
		}

		return nil
	})

	fmt.Println("dump db finished, all items:", totalItems)
}

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
