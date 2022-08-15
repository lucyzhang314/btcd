// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package dumpmdbx

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/dbtool/dumpbtcd/lzma"
)

const (
	chunkSize = 128 * 1024 * 1024 // 512M
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

func decompressFile(fileNameIn, fileNameOut string) {

	decompressFile, err := os.Create(fileNameOut)
	if err != nil {
		return
	}
	defer decompressFile.Close()

	fmt.Println("starting to de-compress file:", fileNameIn)
	loopIdx := 0
	for {
		loopIdx++
		readFilename := buildFilename(fileNameIn, loopIdx)
		inputFile, err := os.Open(readFilename)
		if err != nil {
			break
		}

		lzmaRerader := lzma.NewReader(inputFile)
		buffer := new(bytes.Buffer)
		count, err := io.Copy(buffer, lzmaRerader)
		if err != nil {
			fmt.Println("de-compress failed:", err)
			break
		}
		fmt.Println("de-compress chunk:", loopIdx, count)

		decompressFile.Write(buffer.Bytes())

		lzmaRerader.Close()
		inputFile.Close()
	}

	fmt.Println("de-compress finished:", fileNameIn)
}
