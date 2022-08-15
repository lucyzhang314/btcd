package dumpmdbx

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/btcsuite/btcd/database/ffldb"
)

func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func mkdir(dbPath string) error {
	dbExists := fileExists(dbPath)

	if !dbExists {
		// The error can be ignored here since the call to
		// leveldb.OpenFile will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(dbPath, 0700)
	}

	return nil
}

//
// during dump
// get the last block file in data repo
//
func getLastBlockFile(dirPth, suffix string) (string, error) {
	retName := ""
	err := filepath.Walk(dirPth, func(filename string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}

		lowerName := strings.ToLower(fi.Name())
		if !strings.HasSuffix(lowerName, suffix) {
			return nil
		}
		if strings.Compare(lowerName, retName) > 0 {
			retName = lowerName
		}

		return nil
	})

	return retName, err
}

//
// during restore
// get the first compressed block file in restore dir
//
func getFirCompressedBlockFile(dirPth, suffix string) (string, error) {
	retName := ""
	err := filepath.Walk(dirPth, func(filename string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}

		lowerName := strings.ToLower(fi.Name())
		if !strings.Contains(lowerName, suffix) {
			return nil
		}
		if len(retName) < 1 {
			retName = lowerName
		}
		if strings.Compare(lowerName, retName) < 0 {
			retName = lowerName
		}

		return nil
	})

	return retName, err
}

func buildFilename(in string, idx int) string {
	return fmt.Sprintf("%s.%04d", in, idx)
}

// func copyBlockfile(dbPathDump, dumpFilePath string) {
// 	blockFile, _ := getLastBlockFile(dbPathDump, blockFileSuffix)
// 	if len(blockFile) < 1 {
// 		return
// 	}
// 	fmt.Println("copy block file:", blockFile)
// 	cmd := exec.Command("cp", path.Join(dbPathDump, blockFile), path.Join(dumpFilePath, blockFile))
// 	cmd.Run()
// }
