package dumpmdbx

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"

	_ "github.com/btcsuite/btcd/database/ffldb"
)

const (
	dumpCmd         = "dump"
	restoreCmd      = "restore"
	subBlockFileDir = "mainnet/blocks_ffldb"
	metadataDir     = "metadata"
)

func Start() {
	if len(os.Args) < 4 {
		printUsageInfo()
		return
	}

	if dumpCmd == os.Args[1] {
		sourceDBPath := os.Args[2]
		targeFileName := os.Args[3]
		StartDump(sourceDBPath, targeFileName)
	} else if restoreCmd == os.Args[1] {
		sourceDir := os.Args[2]
		targeDir := os.Args[3]
		StartRestore(context.Background(), sourceDir, targeDir, logrus.NewEntry(logrus.New()))
	} else {
		printUsageInfo()
		return
	}
}

func printUsageInfo() {
	fmt.Println("Usage 1: dbtool dump [source DB directory] [target directory name]")
	fmt.Println("Usage 2: dbtool restore [source directory name] [target DB directory]")
}
