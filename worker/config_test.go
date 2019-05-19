package worker

import (
	"errors"
	"regexp"
	"testing"
)

func TestRun(t *testing.T) {
	reg := regexp.MustCompile(`[:@/]`)
	split := reg.Split("user:password@192.168.56.1:50000/testdb", -1)
	if len(split) != 5{
		return errors.New("the dsn format is failed")
	}
}
