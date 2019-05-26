package worker

import (
	"db2sql/common"
)

type Config struct {
	Dsn     string
	Conc    int
	Number  int
	TranNum int
	Sql     int
	Rate    float64
	MS      []common.MetaData
}
