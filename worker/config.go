package worker

import (
	"db2sql/common"
)

type Config struct {
	Dsn       string
	TabSchema string
	TabName   string
	Conc      int
	Number    int
	TranNum   int
	Sql       int
	CurrCount int
	Rate      float64
	MS        common.MetaData
}
