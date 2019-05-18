package common

import (
	"errors"
	"fmt"
	"log"
	"math/rand"

	"github.com/bxcodec/faker/v3"
)

func Insert(m MetaData) (s string) {
	s += fmt.Sprintf("insert into %s.%s(", m.TabSchema, m.TabName)
	for i, v := range m.Cols {
		if i > 0 {
			s += ", "
		}
		s += v.ColName
	}
	s += ")"

	s += " values("
	for i, v := range m.Cols {
		if i > 0 {
			s += ", "
		}
		switch v.TypeName {
		case "VARCHAR", "CHARACTER":
			var ss string
			if v.KeySeq.Valid {
				ss = faker.UUIDDigit()
			} else {
				ss = faker.Paragraph()
			}
			if int32(len(ss)) > v.Length {
				ss = ss[:v.Length]
			}
			s += fmt.Sprintf("'%s'", ss)
		case "INTEGER":
			s += fmt.Sprintf("%#v", rand.Int31())
		case "TIMESTAMP":
			s += fmt.Sprintf("'%s'", faker.Timestamp())
		case "DECIMAL":
			s += fmt.Sprintf("%f", faker.Longitude())
		default:
			log.Printf("type:%s", v.TypeName)
			panic(errors.New("not support type"))
		}
	}
	s += ")"

	return
}

func Update(m MetaData, kv map[string]interface{}) (s string) {
	s += fmt.Sprintf("update %s.%s set ", m.TabSchema, m.TabName)
	for i, v := range m.Cols {
		if i > 0 {
			s += ","
		}

		switch v.TypeName {
		case "VARCHAR", "CHARACTER":
			if v.Length < 32 {
				s += fmt.Sprintf("%s='%s'", v.ColName, faker.UUIDDigit()[:v.Length])
			} else {
				s += fmt.Sprintf("%s='%s'", v.ColName, faker.UUIDDigit())
			}
		case "INTEGER":
			s += fmt.Sprintf("%s=%#v", v.ColName, rand.Intn(10))
		case "TIMESTAMP":
			s += fmt.Sprintf("%s='%s'", v.ColName, faker.Timestamp())
		case "DECIMAL":
			s += fmt.Sprintf("%s=%f", v.ColName, faker.Longitude())
		default:
			log.Println(">>>", v.TypeName)
			s += fmt.Sprintf("%s=%#v", v.ColName, faker.UUIDDigit())
		}
	}
	s += " where "

	i := 0
	for k, v := range kv {
		s += fmt.Sprintf("%s=%#v", k, v)
		i++
	}

	return
}

func Delete(m MetaData, kv map[string]interface{}) (s string) {
	s += fmt.Sprintf("delete from %s.%s where ", m.TabSchema, m.TabName)

	i := 0
	for k, v := range kv {
		s += fmt.Sprintf("%s=%#v", k, v)
		i++
	}

	return
}
