package main

import (
	//_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/dbtest/dbx"
)

func main() {
	dbx.TryFfldb()
	//dbx.TryMDBX()
	//try_2()
}

//func try_2() {
//	path := filepath.Join(os.TempDir(), "testDrvDev")
//	logger := log.New()
//	db := mdbx.NewMDBX(logger).Path(path).MustOpen()
//	if db == nil {
//		fmt.Println("error")
//		return
//	}
//	defer os.RemoveAll(path)
//	defer db.Close()
//
//	wtx, err := db.BeginRw(context.Background())
//	if err != nil {
//		wtx.Rollback()
//		fmt.Println("something wrong", err)
//		return
//	}
//	bucketName := "bucker01"
//	err = wtx.CreateBucket(bucketName)
//	if err != nil {
//		fmt.Println("something wrong", err)
//		return
//	}
//	err = wtx.CreateBucket(bucketName)
//	if err != nil {
//		fmt.Println("something wrong", err)
//		return
//	}
//	err = wtx.Put(bucketName, []byte("key1"), []byte("lakjsdhflkasjdf"))
//	if err != nil {
//		fmt.Println("something wrong", err)
//		return
//	}
//}
