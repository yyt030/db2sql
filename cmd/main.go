package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"time"

	"db2sql/common"
	"db2sql/worker"
	"github.com/urfave/cli"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	conf := worker.Config{}

	app := cli.NewApp()
	app.Author = "yueyt"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "dsn, d",
			Usage: "the DSN to connect, e.g.: user:passwd@ip:port/dbname",
		},
		cli.StringSliceFlag{
			Name:  "table, t",
			Usage: "table name to operate, e.g.: schema.table",
		},
		cli.IntFlag{
			Name:        "conc, c",
			Value:       1,
			Usage:       "the number of concurrent processor",
			Destination: &conf.Conc,
		},
		cli.IntFlag{
			Name:        "number, n",
			Value:       10,
			Usage:       "the number of all execute sql",
			Destination: &conf.Number,
		},
		cli.IntFlag{
			Name:        "max, m",
			Value:       5,
			Usage:       "the max number of one transaction",
			Destination: &conf.TranNum,
		},
		cli.IntFlag{
			Name:        "sql, s",
			Value:       1,
			Usage:       "the type of sql. e.g.: 1->insert, 2->update, 4->delete, 3->insert+update, 7->insert/update/delte",
			Destination: &conf.Sql,
		},
		cli.Float64Flag{
			Name:        "rate, r",
			Value:       0,
			Usage:       "the rate of rollback. e.g.: 0.5-> half of rollback",
			Destination: &conf.Rate,
		},
	}

	app.Action = func(ctx *cli.Context) error {
		if ctx.String("dsn") == "" || ctx.String("table") == "" {
			cli.ShowAppHelp(ctx)
			return errors.New(fmt.Sprintf("pls input dsn or table"))
		}

		// dsn format is user:passwd@ip:port/dbname
		reg := regexp.MustCompile(`[:@/]`)
		dsnStr := reg.Split(ctx.String("dsn"), -1)
		if len(dsnStr) != 5 {
			cli.ShowAppHelp(ctx)
			return errors.New("dsn format is failed")
		}
		conf.Dsn = fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%s;UID=%s;PWD=%s", dsnStr[2], dsnStr[4], dsnStr[3], dsnStr[0], dsnStr[1])

		// schema.table
		for _, v := range ctx.StringSlice("table") {
			tabStr := strings.Split(v, ".")
			if len(tabStr) != 2 {
				cli.ShowAppHelp(ctx)
				return errors.New("table format is failed")
			}
			tabSchema, tabName := strings.ToUpper(tabStr[0]), strings.ToUpper(tabStr[1])
			// metadata
			ms := common.GetMeta(conf.Dsn, tabSchema, tabName)
			if len(ms.Cols) == 0 {
				return errors.New("can't find the table, Do you input right")
			}
			ms.CurrCount = common.GetCount(conf.Dsn, ms)

			conf.MS = append(conf.MS, ms)
		}

		fmt.Printf("%+v", conf.MS)

		// Sql type
		if conf.Sql > 7 && conf.Sql < 1 {
			return errors.New("sql type is wrong")
		}

		worker.Run(&conf)
		return nil
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("\nerror: %v\n", err)
		os.Exit(1)
	}
}
