package dumpbtcd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
)

func StartRestore() {
	restoreFile := dumpPath // + ".v02"
	restoreDB(restoreFile, dbPathRestore)
}

func restoreDB(restoreFileName, dbPath string) {
	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}

	err = db.Update(func(tx database.Tx) error {

		restorefile, err := os.Open(restoreFileName)
		if err != nil {
			return err
		}
		defer restorefile.Close()
		restoreReader := bufio.NewReader(restorefile)

		for {
			itemCount, bucketName, err2 := readBucketInfo(restoreReader)
			if err2 == io.EOF {
				break
			} else if err2 != nil {
				err = err2
				break
			}

			bucket := getBucket(tx, bucketName)
			if bucket == nil {
				return nil
			}
			fmt.Println("start to restore bucket:", string(bucketName))

			for idx := 0; idx < int(itemCount); idx++ {
				if idx == int(itemCount-1) {
					fmt.Println(idx)
				}
				if (idx % infoStep) == 0 {
					fmt.Println("restore item to:", idx, string(bucketName))
				}

				key, value := readKV(restoreReader)
				if len(key) > 0 {
					bucket.Put(key, value)
				}
			}
		}

		_ = getBucket(tx, []byte(spendjournal))

		fmt.Println("flushing data to DB, please wait a moment")
		return nil
	})

	fmt.Println("finished restore DB", err)
}

func getBucket(tx database.Tx, bucketName []byte) database.Bucket {
	var bucket database.Bucket = nil
	if metadataBucketName == string(bucketName) {
		bucket = tx.Metadata()
	} else if bytes.HasPrefix(bucketName, []byte("cf0")) {
		cfBucket := tx.Metadata().Bucket(cfindexparentbucket)
		if cfBucket == nil {
			cfBucket, _ = tx.Metadata().CreateBucket(cfindexparentbucket)
		}
		bucket = cfBucket.Bucket(bucketName)
		if bucket == nil {
			bucket, _ = cfBucket.CreateBucket(bucketName)
		}
	} else {
		bucket = tx.Metadata().Bucket(bucketName)
		if bucket == nil {
			bucket, _ = tx.Metadata().CreateBucket(bucketName)
		}
	}

	return bucket
}

func readBucketInfo(reader *bufio.Reader) (itemCount uint64, bucketName []byte, err error) {
	buffer64 := make([]byte, 8)
	bufLen, err := reader.Read(buffer64)
	if err == io.EOF {
		return 0, nil, err
	}
	if bufLen != 8 {
		return 0, nil, errors.New("error")
	}

	bucketName, _ = readBytes16(reader)
	itemCount = binary.LittleEndian.Uint64(buffer64)

	return itemCount, bucketName, nil
}

// var loopCount int

func readKV(reader *bufio.Reader) (key, value []byte) {

	// loopCount++

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
