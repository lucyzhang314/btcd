package dumpmdbx

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	mdbxlog "github.com/ledgerwatch/log/v3"
)

const (
// fdbFileSuffix = ".fdb"
)

func StartRestore(restoreDir, targetDir string) {
	firstBlockFile, err := getFirCompressedBlockFile(restoreDir, blockFileSuffix)
	if err != nil {
		fmt.Println("restore DB failed, get fist compressed block file failed")
		return
	}
	// index := strings.Index(firstBlockFile, blockFileSuffix) + len(blockFileSuffix)

	targetDir = path.Join(targetDir, subBlockFileDir)
	mkdir(targetDir)

	// decompress block file
	blockFilename := firstBlockFile[:len(firstBlockFile)-5] // ".0001"
	decompressFile(path.Join(restoreDir, blockFilename), path.Join(targetDir, blockFilename))

	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("btcd%d.bin", time.Now().Unix()))
	defer os.Remove(tmpFile)
	// decompress DB file
	decompressFile(path.Join(restoreDir, compressedFilename), tmpFile)

	dbTargetPath := path.Join(targetDir, metadataDir)
	mkdir(dbTargetPath)
	restoreDB(tmpFile, dbTargetPath)
	// copyBlockfile(restoreDir, dbTargetPath)

	fmt.Println("Congratulations, restore db completed")
}

func restoreDB(restoreFilename, dbTargetPath string) {
	if !fileExists(restoreFilename) {
		fmt.Printf("source dir: %s not existing.\n", restoreFilename)
		return
	}
	// sourceFilename := path.Join(restoreDir, compressedFilename)
	// if !fileExists(sourceFilename) {
	// 	fmt.Printf("source file: %s not existing.\n", sourceFilename)
	// 	return
	// }

	logger := mdbxlog.New()
	// dbpath := path.Join(dbTargetPath, metadataDir)
	// mkdir(dbpath)
	mdb := mdbx.NewMDBX(logger).Path(dbTargetPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			mdbxBucketRoot: kv.TableCfgItem{Flags: kv.Default},
		}
	}).MustOpen()

	fmt.Println("starting to restore DB:", restoreFilename)
	totalItems := uint64(0)
	ctx := context.Background()
	err := mdb.Update(ctx, func(tx kv.RwTx) error {
		restorefile, err := os.Open(restoreFilename)
		if err != nil {
			return err
		}
		defer restorefile.Close()
		restoreReader := bufio.NewReader(restorefile)

		for {
			key, value := readKV(restoreReader)
			if len(key) < 1 {
				break
			}
			tx.Put(mdbxBucketRoot, key, value)

			if (totalItems % infoStep) == 0 {
				fmt.Println("restore DB sequence:", totalItems) //, string(key), string(value))
			}
			totalItems++
		}

		fmt.Println("flushing data to DB, please wait a moment")
		return nil
	})

	fmt.Println("finished restore DB, totalItems:", totalItems, err)
}

func readKV(reader *bufio.Reader) (key, value []byte) {

	key1, err1 := readBytes16(reader)
	value1, err2 := readBytes32(reader)
	if err1 != nil || err2 != nil {
		return nil, nil
	}

	return key1, value1
}

func readBytes16(reader *bufio.Reader) (data []byte, err error) {
	bufLen, err := readUint16(reader)
	if err != nil {
		return nil, err
	}

	data, err = readBytesByLength(reader, uint32(bufLen))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func readBytes32(reader *bufio.Reader) (data []byte, err error) {
	bufLen, err := readUint32(reader)
	if err != nil {
		return nil, err
	}

	data, err = readBytesByLength(reader, uint32(bufLen))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func readUint16(reader *bufio.Reader) (uint16, error) {
	count, err := io.ReadFull(reader, bufTwoBytes)
	if err == io.EOF {
		return 0, err
	} else if count != len(bufTwoBytes) {
		return 0, errors.New("error")
	}

	data := binary.LittleEndian.Uint16(bufTwoBytes)
	return data, nil
}

func readUint32(reader *bufio.Reader) (uint32, error) {
	count, err := io.ReadFull(reader, bufFourBytes)
	if err == io.EOF {
		return 0, err
	} else if count != len(bufFourBytes) {
		return 0, errors.New("error")
	}

	data := binary.LittleEndian.Uint32(bufFourBytes)
	return data, nil
}

func readBytesByLength(reader *bufio.Reader, length uint32) (data []byte, err error) {
	data = make([]byte, length)
	count, err := io.ReadFull(reader, data)
	if err == io.EOF {
		return nil, err
	} else if count != len(data) {
		return nil, errors.New("error")
	}

	return data, nil
}
