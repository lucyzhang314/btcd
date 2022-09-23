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
	db_put()
	db_read()
	//db_put2()
	// db_read2()
	// db_test_bucket()
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

	db.Update(context.Background(), func(tx kv.RwTx) error {

		for idx := 0; idx < 3; idx++ {
			ki := []byte(fmt.Sprintf("kex-%d", idx))
			val := []byte(fmt.Sprintf("value-%d", idx))
			// fmt.Println("put value result:", tx.Put(bucket, ki, val))
			tx.Put(bucket, ki, val)

			ki = []byte(fmt.Sprintf("key-%d", idx))
			val = []byte(fmt.Sprintf("value-321-%d", idx))
			// fmt.Println("put value result:", tx.Put(bucket, ki, val))
			tx.Put(bucket, ki, val)

			ki = []byte(fmt.Sprintf("kez-%d", idx))
			val = []byte(fmt.Sprintf("value-321-%d", idx))
			// fmt.Println("put value result:", tx.Put(bucket, ki, val))
			tx.Put(bucket, ki, val)
		}
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

	db.View(context.Background(), func(tx kv.Tx) error {

		cursor, _ := tx.Cursor(bucket)
		defer cursor.Close()

		kk, val, err := cursor.Current()
		fmt.Println("--, ", string(kk), string(val), err)
		count, err := cursor.Count()
		fmt.Println("--, ", count, err)

		// kk, val, err = cursor.First()
		// for len(kk) > 0 {
		// 	fmt.Println("--, ", string(kk), string(val), err)
		// 	kk, val, err = cursor.Next()
		// }
		tx.ForEach(bucket, []byte("key"), func(k, v []byte) error {
			fmt.Println("-----, ", string(k), string(v), err)
			return nil
		})
		fmt.Println("---------------------------------------------------------")

		tx.ForPrefix(bucket, []byte("key"), func(k, v []byte) error {
			fmt.Println("-----, ", string(k), string(v), err)
			return nil
		})
		fmt.Println("---------------------------------------------------------")

		kk, val, err = cursor.Seek([]byte("kex"))
		fmt.Println("--, ", string(kk), string(val), err)
		count, err = cursor.Count()
		fmt.Println("--, ", count, err)
		kk, val, err = cursor.Current()
		fmt.Println("--, ", string(kk), string(val), err)

		kk, val, err = cursor.Prev()
		fmt.Println("--, ", string(kk), string(val), err)
		kk, val, err = cursor.Current()
		fmt.Println("--, ", string(kk), string(val), err)

		cursor.First()
		kk, val, err = cursor.Current()
		fmt.Println("--, ", string(kk), string(val), err)

		cursor.Last()
		kk, val, err = cursor.Current()
		fmt.Println("--, ", string(kk), string(val), err)

		fmt.Println("---------------------------------------------------------")

		for ; kk != nil; kk, val, err = cursor.Next() {
			if err != nil {
				return err
			}
			fmt.Println("--, ", string(kk), string(val), err)
		}
		fmt.Println("--------------------------------------------------------- 12345")

		return nil
	})
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
