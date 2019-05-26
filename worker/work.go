package worker

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"

	"db2sql/common"
)

func Run(c *Config) {
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

func work(c *Config) {
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

	var commitNum int
	var num int32
	for i := 0; i < c.Number/c.Conc; i++ {
		var optList []int
		if c.Sql&common.INSERT_MASK == common.INSERT_MASK {
			optList = append(optList, common.INSERT_MASK)
		}
		if c.Sql&common.UPDATE_MASK == common.UPDATE_MASK {
			optList = append(optList, common.UPDATE_MASK)
		}
		if c.Sql&common.DELETE_MASK == common.DELETE_MASK {
			optList = append(optList, common.DELETE_MASK)
		}

		var s string

		optListIndex := rand.Intn(len(optList))
		ms := c.MS[rand.Intn(len(c.MS))]

		switch optList[optListIndex] {
		case common.INSERT_MASK:
			s = common.Insert(ms)
		case common.UPDATE_MASK:
			kv := common.GetRandomKeyValue(db, ms, atomic.LoadInt32(&ms.CurrCount))
			s = common.Update(ms, kv)
		case common.DELETE_MASK:
			kv := common.GetRandomKeyValue(db, ms, atomic.LoadInt32(&ms.CurrCount))
			s = common.Delete(ms, kv)
		default:
			panic(err)
		}
		result, err := tx.Exec(s)
		if err != nil {
			log.Println("ERROR", s, err)
			continue
		}
		affected, err := result.RowsAffected()
		if err != nil {
			continue
		} else {
			switch optList[optListIndex] {
			case common.INSERT_MASK:
				num += int32(affected)
			case common.DELETE_MASK:
				num -= int32(affected)
			}

		}

		log.Printf("afected:%d, sql:%s", affected, s)

		if commitNum >= rand.Intn(c.TranNum) {
			if rand.Float64() < c.Rate {
				if err := tx.Rollback(); err != nil {
					panic(err)
				}
				num = 0
				log.Println("--------------------------------- rollback")
				tx, err = db.Begin()
				if err != nil {
					panic(err)
				}
			} else {
				if err := tx.Commit(); err != nil {
					panic(err)
				}
				atomic.AddInt32(&ms.CurrCount, num)
				num = 0
				log.Println("--------------------------------- commit")

				tx, err = db.Begin()
				if err != nil {
					panic(err)
				}
			}
			commitNum = 0
		} else {
			commitNum++
		}
	}
}
