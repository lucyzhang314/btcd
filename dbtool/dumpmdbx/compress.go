// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package dumpmdbx

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"

	"github.com/btcsuite/btcd/dbtool/dumpbtcd/lzma"
)

const (
	chunkSize = 512 * 1024 * 1024 // 512M
)

func compressFile(fileNameIn, fileNameOut string) {
	inputFile, err := os.Open(fileNameIn)
	if err != nil {
		return
	}
	defer inputFile.Close()

	fmt.Println("starting to compress file:", fileNameIn, " target file:", fileNameOut)
	buffer := make([]byte, chunkSize)
	inputReader := bufio.NewReader(inputFile)

	loopIdx := 0
	for {
		loopIdx++
		read, err := io.ReadFull(inputReader, buffer)
		if err == io.EOF {
			break
		}
		fmt.Println("compress chunk index:", loopIdx, " chunk size:", read)

		compressFilename := buildFilename(fileNameOut, loopIdx)
		compressFile, err := os.Create(compressFilename)
		if err != nil {
			return
		}
		compressWriter := bufio.NewWriter(compressFile)

		lzmaWriter := lzma.NewWriter(compressWriter)
		lzmaWriter.Write(buffer[:read])
		lzmaWriter.Close()
		compressFile.Close()
	}

	fmt.Println("compress finished")
}

func decompressFile(fileNameIn, fileNameOut string, log *logrus.Entry) error {

	decompressFile, err := os.Create(fileNameOut)
	if err != nil {
		return err
	}
	defer decompressFile.Close()

	log.Info("starting to de-compress file:", fileNameIn)
	loopIdx := 0
	for {
		loopIdx++
		readFilename := buildFilename(fileNameIn, loopIdx)
		if !fileExists(readFilename) {
			break
		}
		inputFile, err := os.Open(readFilename)
		if err != nil {
			return err
		}

		lzmaRerader := lzma.NewReader(inputFile)
		buffer := new(bytes.Buffer)
		count, err := io.Copy(buffer, lzmaRerader)
		if err != nil {
			log.Error("de-compress failed:", err)
			return err
		}
		log.Info("de-compress chunk:", loopIdx, count)

		if _, err := decompressFile.Write(buffer.Bytes()); nil != err {
			return err
		}

		lzmaRerader.Close()
		inputFile.Close()
	}
	return err
}
