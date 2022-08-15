package dumpbtcd

// import (
// 	"bufio"
// 	"bytes"
// 	"context"
// 	"encoding/binary"
// 	"fmt"
// 	"io"
// 	"os"
// 	"runtime"

// 	"github.com/btcsuite/btcd/database"
// 	_ "github.com/btcsuite/btcd/database/ffldb"
// 	"github.com/btcsuite/btcd/dbtest/dumpbtcd/lzma"
// 	"github.com/btcsuite/btclog"
// 	mdbx2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
// 	"github.com/ledgerwatch/log/v3"
// 	"github.com/torquem-ch/mdbx-go/mdbx"
// )

// // 快照 1 生成（并压缩） creatastate 2 下载（并验证hash） 3 （解压）导入数据库（含chainstate）statetomdbx 4 从快照执行到最新高度

// func Creatastate() {
// 	runtime.GOMAXPROCS(runtime.NumCPU())
// 	backendLogger := btclog.NewBackend(os.Stdout)
// 	defer os.Stdout.Sync()
// 	logb = backendLogger.Logger("MAIN")
// 	dbLog := backendLogger.Logger("BCDB")
// 	dbLog.SetLevel(btclog.LevelDebug)
// 	database.UseLogger(dbLog)
// 	db, err := loadBlockDB()
// 	if err != nil {
// 		return
// 	}
// 	defer db.Close()
// 	// Read every tx
// 	var val, sizek, sizev, total int64
// 	err = db.View(func(tx database.Tx) error {
// 		serializedData := tx.Metadata().Get(chainStateKeyName)
// 		state, err := deserializeBestChainState(serializedData)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Printf("height %5d, totalTxns %9d, hash %s, workSum %s\r\n", state.height, state.totalTxns, TerminalString(state.hash.CloneBytes()), state.workSum.String())
// 		err = tx.Metadata().ForEachBucket(func(k []byte) error {
// 			fmt.Printf("Bucket: %s \r\n", string(k))
// 			return nil
// 		})
// 		// inspect k v
// 		err = tx.Metadata().ForEachBucket(func(bucketname []byte) error {
// 			total = 0
// 			sizek = 0
// 			sizev = 0
// 			Bucket := tx.Metadata().Bucket(bucketname)
// 			err = Bucket.ForEach(func(k, v []byte) error {
// 				sizek += int64(len(k))
// 				sizev += int64(len(v))
// 				total++
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 			fmt.Printf("bucket %20s total %9d sizekye %5d sizev %5d\r\n", string(bucketname), total, sizek>>20, sizev>>20)
// 			return nil
// 		})

// 		// save utxo to file
// 		utxofile, err := os.Create("d:\\statedb\\utxo.bin")
// 		if err != nil {
// 			return err
// 		}
// 		wutxo := bufio.NewWriter(utxofile)

// 		// 保存serializedData到文件
// 		b2 := make([]byte, 2)
// 		binary.LittleEndian.PutUint16(b2, uint16(len(serializedData)))
// 		wutxo.Write(b2)
// 		wutxo.Write(serializedData)

// 		totalHdrs := int(state.height)
// 		totalutxo := 0
// 		sizek = 0
// 		sizev = 0
// 		maxv := 0
// 		maxk := 0
// 		utxoBucket := tx.Metadata().Bucket(utxoSetBucketName)

// 		// 保存utxo到文件
// 		err = utxoBucket.ForEach(func(k, v []byte) error {
// 			sizek += int64(len(k))
// 			sizev += int64(len(v))
// 			wutxo.WriteByte(byte(len(k)))
// 			wutxo.Write(k)
// 			binary.LittleEndian.PutUint16(b2, uint16(len(v)))
// 			wutxo.Write(b2)
// 			wutxo.Write(v)
// 			if len(v) > maxv {
// 				maxv = len(v)
// 				fmt.Printf("maxv %9d \r\n", maxv)
// 			}
// 			if len(k) > maxk {
// 				maxk = len(k)
// 				fmt.Printf("maxk %9d \r\n", maxk)
// 			}
// 			ent, _ := deserializeUtxoEntry(v)
// 			val += ent.amount
// 			totalutxo++
// 			return nil
// 		})
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Printf("height %5d, totalTxns %9d, hash %s, workSum %s totalutxos %9d totalbtc %9d sizekye %d sizev %d\r\n", state.height, state.totalTxns, TerminalString(state.hash.CloneBytes()), state.workSum.String(), totalutxo, val, sizek>>20, sizev>>20)
// 		wutxo.Flush()
// 		utxofile.Close()
// 		for i := 0; i < totalHdrs-1; i++ {
// 			hash, err := dbFetchHashByHeight(tx, int32(i))
// 			if err != nil {
// 				break
// 			}

// 			height, err1 := dbFetchHeightByHash(tx, hash)
// 			if err1 != nil {
// 				break
// 			}
// 			fmt.Sprintf("%d", height)
// 		}
// 		return nil
// 	})
// 	return
// }

// func UtxoDB() {
// 	logger := log.New()
// 	db := mdbx2.NewMDBX(logger).Path("d:\\statedb\\utxo").
// 		WriteMap().
// 		Flags(func(flags uint) uint { return flags | mdbx.NoMemInit }).
// 		MustOpen()
// 	defer db.Close()
// 	wtx, err := db.BeginRw(context.Background())
// 	if err != nil {
// 		wtx.Rollback()
// 		panic(err)
// 	}
// 	wtx.CreateBucket("utxosetv2")
// 	utxofile, err := os.Open("d:\\statedb\\utxo.bin")
// 	if err != nil {
// 		return
// 	}
// 	rutxo := bufio.NewReader(utxofile)

// 	// 从文件恢复serializedData
// 	b2 := make([]byte, 2)
// 	lenk, err := rutxo.ReadByte()

// 	// 从文件恢复utxo
// 	b2 := make([]byte, 2)
// 	for {
// 		lenk, err := rutxo.ReadByte()
// 		if err == io.EOF { // io.EOF 文件的末尾 ?
// 			break
// 		}
// 		key := make([]byte, lenk)
// 		io.ReadFull(rutxo, key)
// 		io.ReadFull(rutxo, b2)
// 		lenv := binary.LittleEndian.Uint16(b2)
// 		val := make([]byte, lenv)
// 		io.ReadFull(rutxo, val)
// 		if err1 := wtx.Put("utxo", key, val); err1 != nil { // tmp
// 			fmt.Printf("put err : %v", err1)
// 		}
// 	}
// 	wtx.Commit()
// 	utxofile.Close()
// }

// func Lzmadecoder(src []byte) []byte {
// 	b := new(bytes.Buffer)
// 	b = new(bytes.Buffer)
// 	in := bytes.NewBuffer(src)
// 	r := lzma.NewReader(in)
// 	defer r.Close()
// 	b.Reset()
// 	n, err := io.Copy(b, r)
// 	if err != nil {
// 		fmt.Printf("put err : %v", err)
// 	}
// 	return b.Bytes()
// }

// // 测试存压缩、解压缩
// func Buflzma() {
// 	src := []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
// 	res := Lzmaencoder(src, 9)
// 	res = Lzmadecoder(res)
// }
