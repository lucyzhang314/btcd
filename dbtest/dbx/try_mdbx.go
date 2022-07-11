package dbx

import (
	"fmt"
	_ "github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	log "github.com/ledgerwatch/log/v3"
	"os"
	"path/filepath"
)

func TryMDBX() {
	//try_1()
	try_2()
}

func try_2() {

	path := filepath.Join(os.TempDir(), "testDrvDev")
	logger := log.New()
	db := mdbx.NewMDBX(logger).Path(path).MustOpen()
	if db == nil {
		fmt.Println("error")
	}

	//db.BeginRw()
}

//func try_1() {
//	env, err1 := mdbx.NewEnv()
//	if err1 != nil {
//		fmt.Println("Cannot create environment: %s", err1)
//	}
//	err1 = env.SetGeometry(-1, -1, 1024*1024, -1, -1, 4096)
//	if err1 != nil {
//		fmt.Println("Cannot set mapsize: %s", err1)
//	}
//	//path := os.TempDir()
//	path := filepath.Join(os.TempDir(), "testDrvDev")
//	err1 = env.Open(path, 0, 0664)
//	defer env.Close()
//	if err1 != nil {
//		fmt.Println("Cannot open environment: %s", err1)
//		return
//	}
//
//	numEntries := 10
//	var data = map[string]string{}
//	var key string
//	var val string
//	for i := 0; i < numEntries; i++ {
//		key = fmt.Sprintf("Key-%d", i)
//		val = fmt.Sprintf("Val-%d", i)
//		data[key] = val
//	}
//	err := env.Update(func(txn *mdbx.Txn) (err error) {
//		var db mdbx.DBI
//		db, err = txn.OpenRoot(0)
//		if err != nil {
//			return err
//		}
//
//		for k, v := range data {
//			err = txn.Put(db, []byte(k), []byte(v), mdbx.NoOverwrite)
//			if err != nil {
//				return fmt.Errorf("put: %v", err)
//			}
//		}
//
//		return nil
//	})
//	if err != nil {
//		fmt.Println(err)
//	}
//
//	stat, err1 := env.Stat()
//	if err1 != nil {
//		fmt.Println("Cannot get stat %s", err1)
//	} else if stat.Entries != uint64(numEntries) {
//		fmt.Errorf("Less entry in the database than expected: %d <> %d", stat.Entries, numEntries)
//	}
//	fmt.Println("%#v", stat)
//
//	err = env.View(func(txn *mdbx.Txn) error {
//		var db mdbx.DBI
//		db, err = txn.OpenRoot(0)
//		if err != nil {
//			return err
//		}
//		cursor, err := txn.OpenCursor(db)
//		if err != nil {
//			cursor.Close()
//			return fmt.Errorf("cursor: %v", err)
//		}
//		var bkey, bval []byte
//		var bNumVal int
//		for {
//			bkey, bval, err = cursor.Get(nil, nil, mdbx.Next)
//			if mdbx.IsNotFound(err) {
//				break
//			}
//			if err != nil {
//				return fmt.Errorf("cursor get: %v", err)
//			}
//			bNumVal++
//			skey := string(bkey)
//			sval := string(bval)
//			fmt.Println("Val: %s", sval)
//			fmt.Println("Key: %s", skey)
//			var d string
//			var ok bool
//			if d, ok = data[skey]; !ok {
//				return fmt.Errorf("cursor get: key does not exist %q", skey)
//			}
//			if d != sval {
//				return fmt.Errorf("cursor get: value %q does not match %q", sval, d)
//			}
//		}
//		if bNumVal != numEntries {
//			fmt.Errorf("cursor iterated over %d entries when %d expected", bNumVal, numEntries)
//		}
//		cursor.Close()
//		bval, err = txn.Get(db, []byte("Key-0"))
//		if err != nil {
//			return fmt.Errorf("get: %v", err)
//		}
//		if string(bval) != "Val-0" {
//			return fmt.Errorf("get: value %q does not match %q", bval, "Val-0")
//		}
//		return nil
//	})
//	if err != nil {
//		fmt.Println(err)
//	}
//
//}
