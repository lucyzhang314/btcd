package dumpmdbx

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path"

	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	mdbxlog "github.com/ledgerwatch/log/v3"
)

const (
	// bitcoinNet         = wire.MainNet
	// dbType             = "ffldb"
	// metadataBucketName = "Metadata"
	// dumpFilePath       = "/Users/andy/tmp/btcdDumpData/btcd.bin"
	// dbPathDump         = "/Users/andy/dev/chainData/data.btcd.console.v003/mainnet/blocks_ffldb/metadata"
	// dbPathDump    = "/Users/andy/dev/store/data.btcd.mdbx.v0.0.6/mainnet/blocks_ffldb/metadata"
	// dbPathRestore = "/Users/andy/dev/chainData/data.btcd.vscode/mainnet/blocks_ffldb/metadata"

	// spendjournal   = "spendjournal"
	// utxosetv2      = "utxosetv2"
	mdbxBucketRoot     = "bucketRoot"
	metadataDir        = "metadata"
	tmpFilename        = "mdbx.dump.tmp"
	compressedFilename = "mdbx.dump"
	blockFileSuffix    = ".fdb"

	infoStep = 1024 * 1024
)

var (
	bufTwoBytes  = make([]byte, 2)
	bufFourBytes = make([]byte, 4)
)

func StartDump(dbPathDump, dumpFilePath string) {
	dumpDB(dbPathDump, dumpFilePath)
	copyBlockfile(dbPathDump, dumpFilePath)

	// comfil := "/Users/andy/dev/btcd/dbtest/tmp/000000000.fdb"
	// compressFile(comfil, comfil+".bin")

	fmt.Println("Congratulations, dump db completed")
}

func dumpDB(dbPathDump, dumpFilePath string) {
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

	// save utxo to file
	mkdir(dumpFilePath)
	dumpFile, err := os.Create(path.Join(dumpFilePath, tmpFilename))
	if err != nil {
		fmt.Println("open target file failed:", err)
		return
	}
	defer dumpFile.Close()

	dumpWriter := bufio.NewWriter(dumpFile)
	defer dumpWriter.Flush()

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
				fmt.Println("dump K/V sequence:", totalItems) //, string(key), string(val))
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
