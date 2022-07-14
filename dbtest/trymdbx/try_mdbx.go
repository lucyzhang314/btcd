package trymdbx

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/kv"
	_ "github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	log "github.com/ledgerwatch/log/v3"
)

func TryMDBX() {
	//db_put()
	//db_read()
	//db_put2()
	// db_read2()
	db_test_bucket()
}

var (
	dbpath = filepath.Join(os.TempDir(), "testDrvDev3_only_mdbx")
	bucket = "rootBucket"
	//table = "rootBucket"
	buck1 = bucket + "1"
	key   = []byte("key1")
	value = []byte("value1.1")
)

const (
	dbflags = kv.Default
	//dbflags = DupSort
)

func db_put() {
	//fmt.Println("yes!")

	logger := log.New()
	//db := mdbx.NewMDBX(logger).Path(path).MustOpen()
	db := mdbx.NewMDBX(logger).Path(dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			bucket: kv.TableCfgItem{Flags: dbflags},
		}
	}).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return
	}
	//defer tx.Rollback()
	defer tx.Commit()

	c, err := tx.RwCursor(bucket)
	//c, err := tx.RwCursorDupSort(table)
	if err != nil {
		return
	}
	//require.NoError(t, err)
	defer c.Close()

	err = c.Put(key, value)
	if err != nil {
		fmt.Println(err)
		return
	}

	key1, value1, err := c.Seek(key)
	fmt.Println(key1, value1, err)
}

func db_test_bucket() {

	logger := log.New()
	//db := mdbx.NewMDBX(logger).Path(path).MustOpen()
	db := mdbx.NewMDBX(logger).Path(dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			bucket: kv.TableCfgItem{Flags: dbflags},
			buck1:  kv.TableCfgItem{Flags: dbflags, IsDeprecated: true},
		}
	}).MustOpen()
	defer db.Close()

	db.Update(context.Background(), func(tx kv.RwTx) error {

		bks, err := tx.ListBuckets()
		fmt.Println(bks, err)

		// err = tx.CreateBucket(buck1)
		// fmt.Println(err)

		ok, err := tx.ExistsBucket(buck1)
		fmt.Println(ok, err)

		// err = tx.Put(bucket, key, value)

		// err = tx.DropBucket(buck1)
		// fmt.Println(err)

		// ok, err = tx.ExistsBucket(buck1)
		// fmt.Println(ok, err)

		// bks, err = tx.ListBuckets()
		// fmt.Println(bks, err)

		// err = tx.Put(buck1, key, value)
		// fmt.Println(err)

		// for idx := 0; idx < 3; idx++ {
		// 	keyi := []byte(fmt.Sprintf("key_%d", idx))
		// 	vali := []byte(fmt.Sprintf("value_%d", idx))
		// 	err = tx.Put(buck1, keyi, vali)
		// 	fmt.Println(err)
		// }

		cursor, err := tx.RwCursor(buck1)
		fmt.Println(cursor, err)
		cnt, err := cursor.Count()
		fmt.Println(cnt, err)

		kk, vv, err := cursor.First()
		loop := uint64(1)
		for loop < cnt {
			fmt.Println(string(kk), string(vv), err)
			kk, vv, err = cursor.Next()
			loop++
		}
		cursor.Close()

		cursor, err = tx.RwCursor(bucket)
		fmt.Println(cursor, err)
		cnt, err = cursor.Count()
		fmt.Println(cnt, err)
		cursor.Close()

		// tx.ClearBucket(buck1)
		// tx.DropBucket(buck1)
		// ok, err = tx.Has(buck1, key)
		// fmt.Println(ok, err)

		// val, err := tx.GetOne(buck1, key)
		// fmt.Println(err, val)

		return nil
	})

}

func db_read2() {
	logger := log.New()
	//db := mdbx.NewMDBX(logger).Path(path).MustOpen()
	db := mdbx.NewMDBX(logger).Path(dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			bucket: kv.TableCfgItem{Flags: dbflags},
			//buck1: kv.TableCfgItem{Flags: dbflags},
		}
	}).MustOpen()
	defer db.Close()

	db.View(context.Background(), func(tx kv.Tx) error {

		val, err := tx.GetOne(buck1, key)
		fmt.Println(val, err)

		return nil
	})
}

func db_read() {
	logger := log.New()
	//db := mdbx.NewMDBX(logger).Path(path).MustOpen()
	db := mdbx.NewMDBX(logger).Path(dbpath).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			bucket: kv.TableCfgItem{Flags: dbflags},
		}
	}).MustOpen()
	defer db.Close()

	//db.Update(context.Background(), func(tx kv.RwTx) error {
	//
	//	tx.Put()
	//	return nil
	//})
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return
	}
	defer tx.Rollback()

	c, err := tx.Cursor(bucket)
	if err != nil {
		return
	}
	defer c.Close()

	c.First()

	key1, value1, err := c.Current()
	fmt.Println(string(key1), string(value1), err)

	key1, value1, err = c.Seek(key)
	fmt.Println(string(key1), string(value1), err)

	key1, value1, err = c.Next()
	fmt.Println(string(key1), string(value1), err)
}
