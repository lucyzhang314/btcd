package main

import (
	"github.com/btcsuite/btcd/dbtest/dumpbtcd"
)

//_ "github.com/btcsuite/btcd/database/ffldb"

func main() {
	// ffldbtry.TryFfldb()
	// trymdbx.TryMDBX()
	//try_2()

	dumpbtcd.StartDump()
	// dumpbtcd.StartRestore()
}
