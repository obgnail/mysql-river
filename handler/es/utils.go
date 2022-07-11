package es

import (
	"bytes"

	"github.com/PuerkitoBio/goquery"
	"github.com/juju/errors"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func HtmlStrip(input interface{}) (string, error) {
	buf := bytes.NewBuffer(nil)
	buf.Reset()
	switch value := input.(type) {
	case []byte:
		buf.Write(value)
	case string:
		buf.WriteString(value)
	default:
	}
	doc, e := goquery.NewDocumentFromReader(buf)
	if e != nil {
		return "", e
	}
	return doc.Text(), nil
}

func IsStringSliceDiff(str1 []string, str2 []string) bool {
	len1 := len(str1)
	len2 := len(str2)
	if len1 != len2 {
		return true
	}
	for i, str := range str1 {
		if str != str2[i] {
			return true
		}
	}
	return false
}

func ParseMysqlGTIDSet(str string) (*mysql.MysqlGTIDSet, error) {
	gtidSet, err := mysql.ParseMysqlGTIDSet(str)
	if err != nil {
		return nil, errors.Trace(err)
	}

	masterGTIDSet, ok := gtidSet.(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, errors.New("failed to convert master gtid set")
	}
	return masterGTIDSet, nil
}

func GetStopPosByUUIDSet(uuidSet *mysql.UUIDSet) int64 {
	var stop int64
	for _, interval := range uuidSet.Intervals {
		if interval.Stop > stop {
			stop = interval.Stop
		}
	}
	return stop
}

func EquelMysqlGTIDSet(set1, set2 *mysql.MysqlGTIDSet) bool {
	return set1.Equal(mysql.GTIDSet(set2))
}
