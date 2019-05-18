package common

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"

	_ "github.com/ibmdb/go_ibm_db"
)

type MetaData struct {
	TabSchema string
	TabName   string
	Cols      []Col
	KeyCols   []string
}

type Col struct {
	ColName  string
	ColNo    int16
	TypeName string
	Length   int32
	Scale    int16
	Nulls    string
	KeySeq   sql.NullString
}

const CONNSTR = "HOSTNAME=192.168.31.128;DATABASE=TESTDB;PORT=50000;UID=db2inst1;PWD=db2inst1"

func NewDB(dsn string) (db *sql.DB, err error) {
	if db, err = sql.Open("go_ibm_db", dsn); err != nil {
		log.Println(err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		log.Println(err)
		return nil, err
	}
	return
}

func GetMeta(db *sql.DB, tabschema, tabname string) (ms MetaData) {
	sqlstr := `select colname, colno, typename, length, scale, nulls, keyseq from syscat.columns where tabschema='%s' and tabname='%s' order by colno`
	s := fmt.Sprintf(sqlstr, tabschema, tabname)
	rows, err := db.Query(s)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var c Col
		if err := rows.Scan(&c.ColName, &c.ColNo, &c.TypeName, &c.Length, &c.Scale, &c.Nulls, &c.KeySeq); err != nil {
			panic(err)
		}

		ms.Cols = append(ms.Cols, c)
		if c.KeySeq.Valid {
			ms.KeyCols = append(ms.KeyCols, c.ColName)
		}
	}
	ms.TabSchema = tabschema
	ms.TabName = tabname

	if ms.KeyCols == nil && ms.Cols != nil {
		ms.KeyCols = []string{ms.Cols[0].ColName}
	}
	return
}

func GetRandomKeyValue(db *sql.DB, ms MetaData, limit int) map[string]interface{} {
	s := fmt.Sprintf("select %s from %s.%s limit %d,1", strings.Join(ms.KeyCols, ","), ms.TabSchema, ms.TabName, rand.Intn(limit))
	rows, err := db.Query(s)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("rows close failed:%v", err)
		}
	}()

	m := make(map[string]interface{})

	for rows.Next() {
		values := make([]interface{}, len(ms.KeyCols))
		valuesPtr := make([]interface{}, len(ms.KeyCols))
		for i := range ms.KeyCols {
			valuesPtr[i] = &values[i]
		}

		if err := rows.Scan(valuesPtr...); err != nil {
			panic(err)
		}

		for i := range ms.KeyCols {
			if v, ok := values[i].([]byte); ok {
				m[ms.KeyCols[i]] = string(v)
			} else {
				m[ms.KeyCols[i]] = values[i]
			}
		}
	}
	return m
}

func GetCount(db *sql.DB, ms MetaData) (n int) {
	s := fmt.Sprintf(`select count(1) from %s.%s with ur`, ms.TabSchema, ms.TabName)
	rows, err := db.Query(s)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("row cloese failed:%v", err)
		}
	}()

	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			panic(err)
		}
	}
	return
}
