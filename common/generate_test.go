package common

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	db, err := NewDB(CONNSTR)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ms := GetMeta(CONNSTR, "DB2INST1", "TEST")
	for i := 0; i < 100; i++ {
		s := Insert(ms)
		log.Printf(">>>%s", s)
		_, err := db.Exec(s)
		if err != nil {
			panic(err)
		}
	}

}

func TestUpdate(t *testing.T) {
	db, err := NewDB(CONNSTR)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ms := GetMeta(CONNSTR, "DB2INST1", "TEST")
	kv := GetRandomKeyValue(db, ms, GetCount(db, ms))

	log.Printf(">>> %s", Update(ms, kv))
}

func TestDelete(t *testing.T) {
	db, err := NewDB(CONNSTR)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	ms := GetMeta(CONNSTR, "DB2INST1", "TEST")
	kv := GetRandomKeyValue(db, ms, GetCount(db, ms))

	log.Printf(">>> %s", Delete(ms, kv))
}
