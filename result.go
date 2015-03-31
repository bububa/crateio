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

func (this *Result) row(idx int, key string) (interface{}, error) {
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

func (this *Result) Int(idx int, key string) (int, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return int(val.(uint)), nil
	case int:
		return val.(int), nil
	case int64:
		return int(val.(int64)), nil
	case int32:
		return int(val.(int32)), nil
	case int16:
		return int(val.(int16)), nil
	case int8:
		return int(val.(int8)), nil
	case uint8:
		return int(val.(uint8)), nil
	case uint16:
		return int(val.(uint16)), nil
	case uint32:
		return int(val.(uint32)), nil
	case uint64:
		return int(val.(uint64)), nil
	case float64:
		return int(val.(float64)), nil
	case float32:
		return int(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseInt(val.(string), 10, 32)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0, nil
			}
			return int(n), nil
		}
		return int(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return 0, nil
}

func (this *Result) Uint(idx int, key string) (uint, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return val.(uint), nil
	case int:
		return uint(val.(int)), nil
	case int64:
		return uint(val.(int64)), nil
	case int32:
		return uint(val.(int32)), nil
	case int16:
		return uint(val.(int16)), nil
	case int8:
		return uint(val.(int8)), nil
	case uint8:
		return uint(val.(uint8)), nil
	case uint16:
		return uint(val.(uint16)), nil
	case uint32:
		return uint(val.(uint32)), nil
	case uint64:
		return uint(val.(uint64)), nil
	case float64:
		return uint(val.(float64)), nil
	case float32:
		return uint(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 32)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0, nil
			}
			return uint(n), nil
		}
		return uint(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return 0, nil
}

func (this *Result) Int64(idx int, key string) (int64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return int64(val.(uint)), nil
	case int:
		return int64(val.(int)), nil
	case int64:
		return val.(int64), nil
	case int32:
		return int64(val.(int32)), nil
	case int16:
		return int64(val.(int16)), nil
	case int8:
		return int64(val.(int8)), nil
	case uint8:
		return int64(val.(uint8)), nil
	case uint16:
		return int64(val.(uint16)), nil
	case uint32:
		return int64(val.(uint32)), nil
	case uint64:
		return int64(val.(uint64)), nil
	case float64:
		return int64(val.(float64)), nil
	case float32:
		return int64(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseInt(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0, nil
			}
			return int64(n), nil
		}
		return int64(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return val.(int64), nil
}

func (this *Result) Uint64(idx int, key string) (uint64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return uint64(val.(uint)), nil
	case int:
		return uint64(val.(int)), nil
	case int64:
		return uint64(val.(int64)), nil
	case int32:
		return uint64(val.(int32)), nil
	case int16:
		return uint64(val.(int16)), nil
	case int8:
		return uint64(val.(int8)), nil
	case uint8:
		return uint64(val.(uint8)), nil
	case uint16:
		return uint64(val.(uint16)), nil
	case uint32:
		return uint64(val.(uint32)), nil
	case uint64:
		return val.(uint64), nil
	case float64:
		return uint64(val.(float64)), nil
	case float32:
		return uint64(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0, nil
			}
			return uint64(n), nil
		}
		return uint64(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return val.(uint64), nil
}

func (this *Result) Uint32(idx int, key string) (uint32, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return uint32(val.(uint)), nil
	case int:
		return uint32(val.(int)), nil
	case int64:
		return uint32(val.(int64)), nil
	case int32:
		return uint32(val.(int32)), nil
	case int16:
		return uint32(val.(int16)), nil
	case int8:
		return uint32(val.(int8)), nil
	case uint8:
		return uint32(val.(uint8)), nil
	case uint16:
		return uint32(val.(uint16)), nil
	case uint32:
		return val.(uint32), nil
	case uint64:
		return uint32(val.(uint64)), nil
	case float64:
		return uint32(val.(float64)), nil
	case float32:
		return uint32(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseUint(val.(string), 10, 64)
		if e != nil {
			n, e := strconv.ParseFloat(val.(string), 64)
			if e != nil {
				return 0, nil
			}
			return uint32(n), nil
		}
		return uint32(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return val.(uint32), nil
}

func (this *Result) Float32(idx int, key string) (float32, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return float32(val.(uint)), nil
	case int:
		return float32(val.(int)), nil
	case int64:
		return float32(val.(int64)), nil
	case int32:
		return float32(val.(int32)), nil
	case int16:
		return float32(val.(int16)), nil
	case int8:
		return float32(val.(int8)), nil
	case uint8:
		return float32(val.(uint8)), nil
	case uint16:
		return float32(val.(uint16)), nil
	case uint32:
		return float32(val.(uint32)), nil
	case uint64:
		return float32(val.(uint64)), nil
	case float64:
		return float32(val.(float64)), nil
	case float32:
		return val.(float32), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseFloat(val.(string), 64)
		if e != nil {
			n, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				return 0, nil
			}
			return float32(n), nil
		}
		return float32(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return val.(float32), nil
}

func (this *Result) Float64(idx int, key string) (float64, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return 0, err
	}
	switch val.(type) {
	case uint:
		return float64(val.(uint)), nil
	case int:
		return float64(val.(int)), nil
	case int64:
		return float64(val.(int64)), nil
	case int32:
		return float64(val.(int32)), nil
	case int16:
		return float64(val.(int16)), nil
	case int8:
		return float64(val.(int8)), nil
	case uint8:
		return float64(val.(uint8)), nil
	case uint16:
		return float64(val.(uint16)), nil
	case uint32:
		return float64(val.(uint32)), nil
	case uint64:
		return float64(val.(uint64)), nil
	case float64:
		return val.(float64), nil
	case float32:
		return float64(val.(float32)), nil
	case nil:
		return 0, nil
	case string:
		n, e := strconv.ParseFloat(val.(string), 64)
		if e != nil {
			n, e := strconv.ParseInt(val.(string), 10, 64)
			if e != nil {
				return 0, nil
			}
			return float64(n), nil
		}
		return float64(n), nil
	case bool:
		switch val.(bool) {
		case true:
			return 1, nil
		case false:
			return 0, nil
		}
	}
	return val.(float64), nil
}

func (this *Result) Str(idx int, key string) (string, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return "", err
	}
	switch val.(type) {
	case uint:
		return strconv.FormatUint(uint64(val.(uint)), 10), nil
	case int:
		return strconv.FormatInt(int64(val.(int)), 10), nil
	case int64:
		return strconv.FormatInt(val.(int64), 10), nil
	case int32:
		return strconv.FormatInt(int64(val.(int32)), 10), nil
	case int16:
		return strconv.FormatInt(int64(val.(int16)), 10), nil
	case int8:
		return strconv.FormatInt(int64(val.(int8)), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val.(uint8)), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val.(uint16)), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val.(uint32)), 10), nil
	case uint64:
		return strconv.FormatUint(val.(uint64), 10), nil
	case float64:
		return strconv.FormatFloat(val.(float64), 'f', 'f', 64), nil
	case float32:
		return strconv.FormatFloat(float64(val.(float32)), 'f', 'f', 32), nil
	case nil:
		return "", nil
	case string:
		return val.(string), nil
	case bool:
		return strconv.FormatBool(val.(bool)), nil
	}
	return val.(string), nil
}

func (this *Result) Time(idx int, key string) (time.Time, error) {
	val, err := this.row(idx, key)
	if err != nil {
		return time.Unix(0, 0), err
	}
	switch val.(type) {
	case uint:
		return time.Unix(int64(val.(uint))/1000, 0), nil
	case int:
		return time.Unix(int64(val.(int))/1000, 0), nil
	case int64:
		return time.Unix(val.(int64)/1000, 0), nil
	case int32:
		return time.Unix(int64(val.(int32))/1000, 0), nil
	case int16:
		return time.Unix(int64(val.(int16))/1000, 0), nil
	case int8:
		return time.Unix(int64(val.(int8))/1000, 0), nil
	case uint8:
		return time.Unix(int64(val.(uint8))/1000, 0), nil
	case uint16:
		return time.Unix(int64(val.(uint16))/1000, 0), nil
	case uint32:
		return time.Unix(int64(val.(uint32))/1000, 0), nil
	case uint64:
		return time.Unix(int64(val.(uint64))/1000, 0), nil
	case float64:
		return time.Unix(int64(val.(float64))/1000, 0), nil
	case float32:
		return time.Unix(int64(val.(float32))/1000, 0), nil
	case nil:
		return time.Unix(0, 0), nil
	case string:
		for _, format := range strToTimeFormats {
			t, err := time.Parse(format, val.(string))
			if err == nil {
				return t, nil
			}
		}
	case bool:
		return time.Unix(0, 0), nil
	}
	return time.Unix(0, 0), nil
}

func (this *Result) Date(idx int, key string) (time.Time, error) {
	t, err := this.Time(idx, key)
	if err != nil {
		return time.Unix(0, 0), err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}
