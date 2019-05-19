package worker

import (
	"log"
	"math/rand"
	"sync"

	"db2sql/common"
)

func Run(c Config) {
	var wg sync.WaitGroup
	for i := 0; i < c.Conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			work(c)
		}()
	}

	wg.Wait()
}

func work(c Config) {
	db, err := common.NewDB(c.Dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	for i := 0; i < c.Number/c.Conc; i++ {
		var s string
		switch {
		case c.Sql&common.INSERT_MASK == common.INSERT_MASK:
			s = common.Insert(c.MS)
		case c.Sql&common.UPDATE_MASK == common.UPDATE_MASK:
			kv := common.GetRandomKeyValue(db, c.MS, c.CurrCount)
			s = common.Update(c.MS, kv)
		case c.Sql&common.DELETE_MASK == common.DELETE_MASK:
			kv := common.GetRandomKeyValue(db, c.MS, c.CurrCount)
			s = common.Delete(c.MS, kv)
		default:
			panic(err)
		}
		result, err := tx.Exec(s)
		if err != nil {
			panic(err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			panic(err)
		}
		log.Println(">>>", affected)

		if rand.Float64() < c.Rate {
			if err := tx.Rollback(); err != nil {
				panic(err)
			}
			log.Println("rollback")
			tx, err = db.Begin()
			if err != nil {
				panic(err)
			}
		} else {
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			log.Println("commit")
			tx, err = db.Begin()
			if err != nil {
				panic(err)
			}
		}

	}
}
