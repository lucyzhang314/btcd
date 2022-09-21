package dumpmdbx

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/btcsuite/btclog"
	"io"
	"os"
	"path"

	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	mdbxlog "github.com/ledgerwatch/log/v3"
)

func StartRestore(restoreDir, targetDir string, log btclog.Logger) error {
	firstBlockFile, err := getFirstCompressedBlockFile(restoreDir, blockFileSuffix)
	if err != nil {
		log.Error("restore DB failed, get fist compressed block file failed")
		return err
	}

	// targetDir = path.Join(targetDir, subBlockFileDir)
	if err := mkdir(targetDir); nil != err {
		return err
	}

	// decompress block file
	blockFilename := firstBlockFile[:len(firstBlockFile)-5] // ".0001"
	if err := decompressFile(path.Join(restoreDir, blockFilename), path.Join(targetDir, blockFilename)); nil != err {
		return err
	}

	tmpFile := generateTempFilename()
	defer os.Remove(tmpFile)
	// decompress DB to a temporary file
	if err := decompressFile(path.Join(restoreDir, compressedFilename), tmpFile); nil != err {
		return err
	}

	dbTargetPath := path.Join(targetDir, metadataDir)
	if err := mkdir(dbTargetPath); nil != err {
		return err
	}

	createDB(dbTargetPath)
	if err := createBuckets(targetDir); nil != err {
		return err
	}

	// restore DB
	return restoreDB(tmpFile, dbTargetPath, log)
}

func createDB(dbTargetPath string) {
	logger := mdbxlog.New()
	mdb := mdbx.NewMDBX(logger).Path(dbTargetPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			mdbxBucketRoot: kv.TableCfgItem{Flags: kv.Default},
		}
	}).MustOpen()
	defer mdb.Close()
}

func restoreDB(restoreFilename, dbTargetPath string, log btclog.Logger) error {
	if !fileExists(restoreFilename) {
		return fmt.Errorf("source dir: %s not existing.\n", restoreFilename)
	}

	logger := mdbxlog.New()
	mdb := mdbx.NewMDBX(logger).Path(dbTargetPath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			mdbxBucketRoot: kv.TableCfgItem{Flags: kv.Default},
		}
	}).MustOpen()
	defer mdb.Close()

	log.Info("starting to restore DB:", restoreFilename)
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
			if err := tx.Put(mdbxBucketRoot, key, value); nil != err {
				return err
			}

			if (totalItems % infoStep) == 0 {
				log.Info("restore DB sequence:", totalItems) //, string(key), string(value))
			}
			totalItems++
		}

		log.Info("flushing data to DB, please wait a moment")
		return nil
	})

	log.Info("finished restore DB, totalItems:", totalItems, err)
	return err
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
