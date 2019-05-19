package common

import (
	"log"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestGetRandomKeyValue(t *testing.T) {
	db, err := NewDB(CONNSTR)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ms := GetMeta(CONNSTR, "DB2INST1", "TEST")

	m := GetRandomKeyValue(db, ms, GetCount(db, ms))
	log.Printf("%+v", m)
}
