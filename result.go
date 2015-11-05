package crateio

import (
	"errors"
	"strconv"
	"time"
)

var strToTimeFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.000000000",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02T15:04:05.000000000",
	"01/02/2006",
	"01/02/2006 15:04",
	"01/02/2006 15:04:05",
	"01/02/2006 15:04:05.000000000",
	"01/02/06",
	"01/02/06 15:04",
	"01/02/06 15:04:05",
	"01/02/06 15:04:05.000000000",
	"Mon Jan _2 15:04:05.000000000 -0700 MST 2006",
	"_2/Jan/2006 15:04:05",
	"Jan _2, 2006",
	"20060102",
	time.ANSIC,
	time.UnixDate,
	time.RubyDate,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC3339,
	time.RFC3339Nano,
	time.Kitchen,
	time.Stamp,
	time.StampMilli,
	time.StampMicro,
	time.StampNano,
}

func (this Result) row(idx int, key string) (interface{}, error) {
	pos := -1
	for i, k := range this.Cols {
		if key != k {
			continue
		}
		pos = i
		break
	}
	if pos == -1 {
		return 0, errors.New("no matched column")
	}
	val := this.Rows[idx][pos]
	return val, nil

}

func (this Result) Int(idx int, key string) (int, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustInt(val), nil
}

func (this Result) Uint(idx int, key string) (uint, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustUint(val), nil
}

func (this Result) Int64(idx int, key string) (int64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustInt64(val), nil
}

func (this Result) Uint64(idx int, key string) (uint64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustUint64(val), nil
}

func (this Result) Uint32(idx int, key string) (uint32, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustUint32(val), nil
}

func (this Result) Float32(idx int, key string) (float32, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustFloat32(val), nil
}

func (this Result) Float64(idx int, key string) (float64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	return mustFloat64(val), nil
}

func (this Result) Str(idx int, key string) (string, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return "", err
	}
	return mustStr(val), nil
}

func (this Result) Time(idx int, key string) (time.Time, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return mustTime(val), nil
}

func (this Result) Date(idx int, key string) (time.Time, error) {
	t, err := this.Time(idx, key)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}

func (this Result) IntSlice(idx int, key string) ([]int, error) {
	var ret []int
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustInt(val))
	}
	return ret, nil
}

func (this Result) UintSlice(idx int, key string) ([]uint, error) {
	var ret []uint
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustUint(val))
	}
	return ret, nil
}

func (this Result) Int64Slice(idx int, key string) ([]int64, error) {
	var ret []int64
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustInt64(val))
	}
	return ret, nil
}

func (this Result) Uint64Slice(idx int, key string) ([]uint64, error) {
	var ret []uint64
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustUint64(val))
	}
	return ret, nil
}

func (this Result) Uint32Slice(idx int, key string) ([]uint32, error) {
	var ret []uint32
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustUint32(val))
	}
	return ret, nil
}

func (this Result) Float32Slice(idx int, key string) ([]float32, error) {
	var ret []float32
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustFloat32(val))
	}
	return ret, nil
}

func (this Result) Float64Slice(idx int, key string) ([]float64, error) {
	var ret []float64
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustFloat64(val))
	}
	return ret, nil
}

func (this Result) StrSlice(idx int, key string) ([]string, error) {
	var ret []string
	arr, err := this.row(idx, key)
	if err != nil {
		return ret, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustStr(val))
	}
	return ret, nil
}

func (this Result) TimeSlice(idx int, key string) ([]time.Time, error) {
	var ret []time.Time
	arr, err := this.row(idx, key)
	if err != nil {
		return nil, err
	}
	for _, val := range arr.([]interface{}) {
		ret = append(ret, mustTime(val))
	}
	return ret, nil
}

func (this Result) DateSlice(idx int, key string) ([]time.Time, error) {
	var ret []time.Time
	arr, err := this.TimeSlice(idx, key)
	if err != nil {
		return nil, err
	}
	for _, t := range arr {
		ret = append(ret, time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()))
	}
	return ret, nil
}

func mustInt(val interface{}) int {

	switch val.(type) {
	case uint:
		return int(val.(uint))
	case int:
		return val.(int)
	case int64:
		return int(val.(int64))
	case int32:
		return int(val.(int32))
	case int16:
		return int(val.(int16))
	case int8:
		return int(val.(int8))
	case uint8:
		return int(val.(uint8))
	case uint16:
		return int(val.(uint16))
	case uint32:
		return int(val.(uint32))
	case uint64:
		return int(val.(uint64))
	case float64:
		return int(val.(float64))
	case float32:
		return int(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseInt(val.(string), 10, 32)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0
			}
			return int(n)
		}
		return int(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return 0
}

func mustUint(val interface{}) uint {
	switch val.(type) {
	case uint:
		return val.(uint)
	case int:
		return uint(val.(int))
	case int64:
		return uint(val.(int64))
	case int32:
		return uint(val.(int32))
	case int16:
		return uint(val.(int16))
	case int8:
		return uint(val.(int8))
	case uint8:
		return uint(val.(uint8))
	case uint16:
		return uint(val.(uint16))
	case uint32:
		return uint(val.(uint32))
	case uint64:
		return uint(val.(uint64))
	case float64:
		return uint(val.(float64))
	case float32:
		return uint(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 32)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0
			}
			return uint(n)
		}
		return uint(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return 0
}

func mustInt64(val interface{}) int64 {
	switch val.(type) {
	case uint:
		return int64(val.(uint))
	case int:
		return int64(val.(int))
	case int64:
		return val.(int64)
	case int32:
		return int64(val.(int32))
	case int16:
		return int64(val.(int16))
	case int8:
		return int64(val.(int8))
	case uint8:
		return int64(val.(uint8))
	case uint16:
		return int64(val.(uint16))
	case uint32:
		return int64(val.(uint32))
	case uint64:
		return int64(val.(uint64))
	case float64:
		return int64(val.(float64))
	case float32:
		return int64(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseInt(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0
			}
			return int64(n)
		}
		return int64(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return val.(int64)
}

func mustUint64(val interface{}) uint64 {
	switch val.(type) {
	case uint:
		return uint64(val.(uint))
	case int:
		return uint64(val.(int))
	case int64:
		return uint64(val.(int64))
	case int32:
		return uint64(val.(int32))
	case int16:
		return uint64(val.(int16))
	case int8:
		return uint64(val.(int8))
	case uint8:
		return uint64(val.(uint8))
	case uint16:
		return uint64(val.(uint16))
	case uint32:
		return uint64(val.(uint32))
	case uint64:
		return val.(uint64)
	case float64:
		return uint64(val.(float64))
	case float32:
		return uint64(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0
			}
			return uint64(n)
		}
		return uint64(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return val.(uint64)
}

func mustUint32(val interface{}) uint32 {
	switch val.(type) {
	case uint:
		return uint32(val.(uint))
	case int:
		return uint32(val.(int))
	case int64:
		return uint32(val.(int64))
	case int32:
		return uint32(val.(int32))
	case int16:
		return uint32(val.(int16))
	case int8:
		return uint32(val.(int8))
	case uint8:
		return uint32(val.(uint8))
	case uint16:
		return uint32(val.(uint16))
	case uint32:
		return val.(uint32)
	case uint64:
		return uint32(val.(uint64))
	case float64:
		return uint32(val.(float64))
	case float32:
		return uint32(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0
			}
			return uint32(n)
		}
		return uint32(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return val.(uint32)
}

func mustFloat32(val interface{}) float32 {

	switch val.(type) {
	case uint:
		return float32(val.(uint))
	case int:
		return float32(val.(int))
	case int64:
		return float32(val.(int64))
	case int32:
		return float32(val.(int32))
	case int16:
		return float32(val.(int16))
	case int8:
		return float32(val.(int8))
	case uint8:
		return float32(val.(uint8))
	case uint16:
		return float32(val.(uint16))
	case uint32:
		return float32(val.(uint32))
	case uint64:
		return float32(val.(uint64))
	case float64:
		return float32(val.(float64))
	case float32:
		return val.(float32)
	case nil:
		return 0
	case string:
		n, e := strconv.ParseFloat(val.(string), 64)
		if e != nil {
			n, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				return 0
			}
			return float32(n)
		}
		return float32(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return val.(float32)
}

func mustFloat64(val interface{}) float64 {
	switch val.(type) {
	case uint:
		return float64(val.(uint))
	case int:
		return float64(val.(int))
	case int64:
		return float64(val.(int64))
	case int32:
		return float64(val.(int32))
	case int16:
		return float64(val.(int16))
	case int8:
		return float64(val.(int8))
	case uint8:
		return float64(val.(uint8))
	case uint16:
		return float64(val.(uint16))
	case uint32:
		return float64(val.(uint32))
	case uint64:
		return float64(val.(uint64))
	case float64:
		return val.(float64)
	case float32:
		return float64(val.(float32))
	case nil:
		return 0
	case string:
		n, e := strconv.ParseFloat(val.(string), 64)
		if e != nil {
			n, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				return 0
			}
			return float64(n)
		}
		return float64(n)
	case bool:
		switch val.(bool) {
		case true:
			return 1
		case false:
			return 0
		}
	}
	return val.(float64)
}

func mustStr(val interface{}) string {
	switch val.(type) {
	case uint:
		return strconv.FormatUint(uint64(val.(uint)), 10)
	case int:
		return strconv.FormatInt(int64(val.(int)), 10)
	case int64:
		return strconv.FormatInt(val.(int64), 10)
	case int32:
		return strconv.FormatInt(int64(val.(int32)), 10)
	case int16:
		return strconv.FormatInt(int64(val.(int16)), 10)
	case int8:
		return strconv.FormatInt(int64(val.(int8)), 10)
	case uint8:
		return strconv.FormatUint(uint64(val.(uint8)), 10)
	case uint16:
		return strconv.FormatUint(uint64(val.(uint16)), 10)
	case uint32:
		return strconv.FormatUint(uint64(val.(uint32)), 10)
	case uint64:
		return strconv.FormatUint(val.(uint64), 10)
	case float64:
		return strconv.FormatFloat(val.(float64), 'f', 'f', 64)
	case float32:
		return strconv.FormatFloat(float64(val.(float32)), 'f', 'f', 32)
	case nil:
		return ""
	case string:
		return val.(string)
	case bool:
		return strconv.FormatBool(val.(bool))
	}
	return val.(string)
}

func mustTime(val interface{}) time.Time {
	switch val.(type) {
	case uint:
		return time.Unix(int64(val.(uint))/1000, 0)
	case int:
		return time.Unix(int64(val.(int))/1000, 0)
	case int64:
		return time.Unix(val.(int64)/1000, 0)
	case int32:
		return time.Unix(int64(val.(int32))/1000, 0)
	case int16:
		return time.Unix(int64(val.(int16))/1000, 0)
	case int8:
		return time.Unix(int64(val.(int8))/1000, 0)
	case uint8:
		return time.Unix(int64(val.(uint8))/1000, 0)
	case uint16:
		return time.Unix(int64(val.(uint16))/1000, 0)
	case uint32:
		return time.Unix(int64(val.(uint32))/1000, 0)
	case uint64:
		return time.Unix(int64(val.(uint64))/1000, 0)
	case float64:
		return time.Unix(int64(val.(float64))/1000, 0)
	case float32:
		return time.Unix(int64(val.(float32))/1000, 0)
	case nil:
		return time.Unix(0, 0)
	case string:
		for _, format := range strToTimeFormats {
			t, err := time.Parse(format, val.(string))
			if err == nil {
				return t
			}
		}
	case bool:
		return time.Unix(0, 0)
	}
	return time.Unix(0, 0)
}
