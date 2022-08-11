package dumpbtcd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/dbtest/dumpbtcd/lzma"
)

func StartRestore() {
	restoreFile := dumpPath + ".decompress"
	restoreDbPath := dbPath + ".res"
	// decompressFile(dumpPath, restoreFile)
	restoreDB(restoreFile, restoreDbPath)
}

func decompressFile(fileNameIn, fileNameOut string) {

	inputFile, err := os.Open(fileNameIn)
	if err != nil {
		return
	}

	defer inputFile.Close()

	lzmaRerader := lzma.NewReader(inputFile)
	defer lzmaRerader.Close()

	buffer := new(bytes.Buffer)
	count, err := io.Copy(buffer, lzmaRerader)
	fmt.Println(count, err)

	// save utxo to file
	decompressFile, err := os.Create(fileNameOut)
	if err != nil {
		return
	}
	defer decompressFile.Close()

	decompressFile.Write(buffer.Bytes())
	fmt.Println("de-compress finished")
}

func restoreDB(restoreFileName, dbPath string) {
	db, err := database.Open(dbType, dbPath, bitcoinNet)
	if err != nil {
		return
	}

	db.Update(func(tx database.Tx) error {

		// utxoSetBucket := tx.Metadata().Bucket(utxoSetBucketName)
		utxoSetBucket, err := tx.Metadata().CreateBucket(utxoSetBucketName)
		if err != nil {
			return err
		}

		restorefile, err := os.Open(restoreFileName)
		if err != nil {
			return nil
		}
		defer restorefile.Close()
		restoreReader := bufio.NewReader(restorefile)

		{
			// restore state
			serializedData := readSerializeData(restoreReader)
			if len(serializedData) < 1 {
				return nil
			}
			tx.Metadata().Put(chainStateKeyName, serializedData)
		}

		for key, value := readKV(restoreReader); len(key) > 0; {
			utxoSetBucket.Put(key, value)
			key, value = readKV(restoreReader)
		}

		fmt.Println("flushing data to DB, please wait a moment")
		return nil
	})

	fmt.Println("finished restore DB")
}

func readSerializeData(reader *bufio.Reader) []byte {

	buffer := make([]byte, 2)
	bufLen, err := reader.Read(buffer)
	if (err == io.EOF) || (bufLen != 2) {
		return nil
	}

	serializeDataLen := binary.LittleEndian.Uint16(buffer)
	serializedData := make([]byte, serializeDataLen)
	bufLen, err = reader.Read(serializedData)
	if (err == io.EOF) || (uint16(bufLen) != serializeDataLen) {
		return nil
	}

	return serializedData
}

var loopCount int

func readKV(reader *bufio.Reader) (key, value []byte) {

	loopCount++
	keyLen, err := reader.ReadByte()
	if err == io.EOF {
		fmt.Println("loop count:", loopCount)
		return nil, nil
	}

	key = make([]byte, keyLen)
	count, err := io.ReadFull(reader, key)
	if (err == io.EOF) || (count != int(keyLen)) {
		fmt.Println("loop count:", loopCount)
		return nil, nil
	}

	valueLenBuf := make([]byte, 2)
	count, err = io.ReadFull(reader, valueLenBuf)
	if (err == io.EOF) || (count != 2) {
		return nil, nil
	}

	valueLen := binary.LittleEndian.Uint16(valueLenBuf)
	value = make([]byte, valueLen)
	count, err = io.ReadFull(reader, value)
	if (err == io.EOF) || (uint16(count) != valueLen) {
		return nil, nil
	}

	return
}
