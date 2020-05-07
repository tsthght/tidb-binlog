package sync

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"


func GenSQL(normal string, args []interface{}, backslashEscapes bool, loc *time.Location) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(normal, "?") != len(args) {
		return "", errors.New("the number of parameters does not match")
	}

	var sql []byte = nil
	argPos := 0

	for i := 0; i < len(normal); i++ {
		q := strings.IndexByte(normal[i:], '?')
		if q == -1 {
			sql = append(sql, normal[i:]...)
			break
		}
		sql = append(sql, normal[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			sql = append(sql, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int:
			sql = strconv.AppendInt(sql, int64(v), 10)
		case int32:
			sql = strconv.AppendInt(sql, int64(v), 10)
		case int64:
			sql = strconv.AppendInt(sql, int64(v), 10)
		case float64:
			sql = strconv.AppendFloat(sql, v, 'g', -1, 64)
		case bool:
			if v {
				sql = append(sql, '1')
			} else {
				sql = append(sql, '0')
			}
		case time.Time:
			if v.IsZero() {
				sql = append(sql, "'0000-00-00'"...)
			} else {
				v := v.In(loc)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				sql = append(sql, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					sql = append(sql, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				sql = append(sql, '\'')
			}
		case []byte:
			if v == nil {
				sql = append(sql, "NULL"...)
			} else {
				//sql = append(sql, "_binary'"...)
				sql = append(sql, "'"...)
				if backslashEscapes {
					sql = escapeBytesBackslash(sql, v)
				} else {
					sql = escapeBytesQuotes(sql, v)
				}
				sql = append(sql, '\'')
			}
		case string:
			sql = append(sql, '\'')
			if backslashEscapes {
				sql = escapeStringBackslash(sql, v)
			} else {
				sql = escapeStringQuotes(sql, v)
			}
			sql = append(sql, '\'')
		default:
			return "", errors.New(fmt.Sprintf("unknown type: %T", v))
		}
	}
	if argPos != len(args) {
		return "", errors.New("wrong number of args")
	}
	return string(sql), nil
}

func main() {

}

func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

