// Copyright Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package brimtext contains tools for working with text. Probably the most
// complex of these tools is Align, which allows for formatting "pretty
// tables".
//
// This is the latest stable version of the package.
//
// For the latest development version of the package, switch to the
// master branch at https://github.com/gholt/brimtext
// or use github.com/gholt/brimtext as the import path.
//
// Also, you'd want to use http://godoc.org/github.com/gholt/brimtext
// for the development documentation.
package brimtext

import (
	"bytes"
	"strconv"
	"strings"
)

// OrdinalSuffix returns "st", "nd", "rd", etc. for the number given (1st, 2nd,
// 3rd, etc.).
func OrdinalSuffix(number int) string {
	if (number/10)%10 == 1 || number%10 > 3 {
		return "th"
	} else if number%10 == 1 {
		return "st"
	} else if number%10 == 2 {
		return "nd"
	} else if number%10 == 3 {
		return "rd"
	}
	return "th"
}

// ThousandsSep returns the number formatted using the separator at each
// thousands position, such as ThousandsSep(1234567, ",") giving 1,234,567.
func ThousandsSep(v int64, sep string) string {
	s := strconv.FormatInt(v, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// ThousandsSepU returns the number formatted using the separator at each
// thousands position, such as ThousandsSepU(1234567, ",") giving 1,234,567.
func ThousandsSepU(v uint64, sep string) string {
	s := strconv.FormatUint(v, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

type humanSize struct {
	d int64
	s string
}

var humanSizes = []humanSize{
	humanSize{int64(1024), "K"},
	humanSize{int64(1024) << 10, "M"},
	humanSize{int64(1024) << 20, "G"},
	humanSize{int64(1024) << 30, "T"},
	humanSize{int64(1024) << 40, "P"},
	humanSize{int64(1024) << 50, "E"},
}

// Returns a more readable size format, such as HumanSize(1234567, "") giving
// "1M". For values less than 1K, it is common that no suffix letter should be
// added; but the appendBytes parameter is provided in case clarity is needed.
func HumanSize(b int64, appendBytes string) string {
	if b < 1024 {
		v := strconv.FormatInt(b, 10)
		if appendBytes != "" {
			return v + appendBytes
		}
		return v
	}
	c := b
	s := appendBytes
	for _, h := range humanSizes {
		c = b / h.d
		r := b % h.d
		if r >= h.d/2 {
			c++
		}
		if c < 1024 {
			s = h.s
			break
		}
	}
	return strconv.FormatInt(c, 10) + s
}

type Alignment int

const (
	Left Alignment = iota
	Right
	Center
)

type AlignOptions struct {
	// Widths indicate the desired widths of each column. If nil or if a value
	// is 0, no rewrapping will be done.
	Widths     []int
	Alignments []Alignment
	// FirstDR etc. control what is output for situations with a prepended
	// display row, First row output with Down and Right connections, etc.
	FirstDR       string
	FirstLR       string
	FirstFirstDLR string
	FirstDLR      string
	FirstDL       string
	// RowFirstUD etc. control situations for each data row output.
	RowFirstUD  string
	RowSecondUD string
	RowUD       string
	RowLastUD   string
	// LeaveTrailingWhitespace should be set true if the last cell of data row
	// needs spaces to fill to the end (usually needed when setting RawLastUD).
	LeaveTrailingWhitespace bool
	// FirstNilFirstUDR etc. control situations when the first nil data row is
	// encountered. Can be used to separate the header from the rest of the
	// rows.
	FirstNilFirstUDR  string
	FirstNilLR        string
	FirstNilFirstUDLR string
	FirstNilUDLR      string
	FirstNilLastUDL   string
	// NilFirstUDR etc. control situations when the second and subsequent nil
	// data rows are encountered. Can be used to separate rows from each other.
	NilFirstUDR  string
	NilLR        string
	NilFirstUDLR string
	NilUDLR      string
	NilLastUDL   string
	// LastUR etc. control what is output for situations with an appended
	// display row.
	LastUR       string
	LastLR       string
	LastFirstULR string
	LastULR      string
	LastUL       string
	// NilBetweenEveryRow will add a nil data row between all rows; use to emit
	// FirstNil* and Nil* row separators.
	NilBetweenEveryRow bool
}

// NewDefaultAlignOptions gives:
//
//  &AlignOptions{RowSecondUD: " ", RowUD: " "}
//
// Which will format tables like:
//
//           Bob         Sue    John
//  Hometown San Antonio Austin New York
//  Mother   Bessie      Mary   Sarah
//  Father   Rick        Dan    Mike
func NewDefaultAlignOptions() *AlignOptions {
	return &AlignOptions{RowSecondUD: " ", RowUD: " "}
}

// NewSimpleAlignOptions gives:
//
//  return &AlignOptions{
//      FirstDR:                 "+-",
//      FirstLR:                 "-",
//      FirstFirstDLR:           "-+-",
//      FirstDLR:                "-+-",
//      FirstDL:                 "-+",
//      RowFirstUD:              "| ",
//      RowSecondUD:             " | ",
//      RowUD:                   " | ",
//      RowLastUD:               " |",
//      LeaveTrailingWhitespace: true,
//      FirstNilFirstUDR:        "+-",
//      FirstNilLR:              "-",
//      FirstNilFirstUDLR:       "-+-",
//      FirstNilUDLR:            "-+-",
//      FirstNilLastUDL:         "-+",
//      LastUR:                  "+-",
//      LastLR:                  "-",
//      LastFirstULR:            "-+-",
//      LastULR:                 "-+-",
//      LastUL:                  "-+",
//  }
//
// Which will format tables like:
//
//  +----------+-------------+--------+----------+
//  |          | Bob         | Sue    | John     |
//  +----------+-------------+--------+----------+
//  | Hometown | San Antonio | Austin | New York |
//  | Mother   | Bessie      | Mary   | Sarah    |
//  | Father   | Rick        | Dan    | Mike     |
//  +----------+-------------+--------+----------+
func NewSimpleAlignOptions() *AlignOptions {
	return &AlignOptions{
		FirstDR:                 "+-",
		FirstLR:                 "-",
		FirstFirstDLR:           "-+-",
		FirstDLR:                "-+-",
		FirstDL:                 "-+",
		RowFirstUD:              "| ",
		RowSecondUD:             " | ",
		RowUD:                   " | ",
		RowLastUD:               " |",
		LeaveTrailingWhitespace: true,
		FirstNilFirstUDR:        "+-",
		FirstNilLR:              "-",
		FirstNilFirstUDLR:       "-+-",
		FirstNilUDLR:            "-+-",
		FirstNilLastUDL:         "-+",
		LastUR:                  "+-",
		LastLR:                  "-",
		LastFirstULR:            "-+-",
		LastULR:                 "-+-",
		LastUL:                  "-+",
	}
}

// NewBoxedAlignOptions gives:
//  &AlignOptions{
//      FirstDR:                 "+=",
//      FirstLR:                 "=",
//      FirstFirstDLR:           "=+=",
//      FirstDLR:                "=+=",
//      FirstDL:                 "=+",
//      RowFirstUD:              "| ",
//      RowSecondUD:             " | ",
//      RowUD:                   " | ",
//      RowLastUD:               " |",
//      LeaveTrailingWhitespace: true,
//      FirstNilFirstUDR:        "+=",
//      FirstNilLR:              "=",
//      FirstNilFirstUDLR:       "=+=",
//      FirstNilUDLR:            "=+=",
//      FirstNilLastUDL:         "=+",
//      NilFirstUDR:             "+-",
//      NilLR:                   "-",
//      NilFirstUDLR:            "-+-",
//      NilUDLR:                 "-+-",
//      NilLastUDL:              "-+",
//      LastUR:                  "+=",
//      LastLR:                  "=",
//      LastFirstULR:            "=+=",
//      LastULR:                 "=+=",
//      LastUL:                  "=+",
//      NilBetweenEveryRow:      true,
//  }
//
// Which will format tables like:
//
//  +==========+=============+========+==========+
//  |          | Bob         | Sue    | John     |
//  +==========+=============+========+==========+
//  | Hometown | San Antonio | Austin | New York |
//  +----------+-------------+--------+----------+
//  | Mother   | Bessie      | Mary   | Sarah    |
//  +----------+-------------+--------+----------+
//  | Father   | Rick        | Dan    | Mike     |
//  +==========+=============+========+==========+
func NewBoxedAlignOptions() *AlignOptions {
	return &AlignOptions{
		FirstDR:                 "+=",
		FirstLR:                 "=",
		FirstFirstDLR:           "=+=",
		FirstDLR:                "=+=",
		FirstDL:                 "=+",
		RowFirstUD:              "| ",
		RowSecondUD:             " | ",
		RowUD:                   " | ",
		RowLastUD:               " |",
		LeaveTrailingWhitespace: true,
		FirstNilFirstUDR:        "+=",
		FirstNilLR:              "=",
		FirstNilFirstUDLR:       "=+=",
		FirstNilUDLR:            "=+=",
		FirstNilLastUDL:         "=+",
		NilFirstUDR:             "+-",
		NilLR:                   "-",
		NilFirstUDLR:            "-+-",
		NilUDLR:                 "-+-",
		NilLastUDL:              "-+",
		LastUR:                  "+=",
		LastLR:                  "=",
		LastFirstULR:            "=+=",
		LastULR:                 "=+=",
		LastUL:                  "=+",
		NilBetweenEveryRow:      true,
	}
}

// NewUnicodeBoxedAlignOptions gives:
//  &AlignOptions{
//      FirstDR:                 "\u2554\u2550",
//      FirstLR:                 "\u2550",
//      FirstFirstDLR:           "\u2550\u2566\u2550",
//      FirstDLR:                "\u2550\u2564\u2550",
//      FirstDL:                 "\u2550\u2557",
//      RowFirstUD:              "\u2551 ",
//      RowSecondUD:             " \u2551 ",
//      RowUD:                   " \u2502 ",
//      RowLastUD:               " \u2551",
//      LeaveTrailingWhitespace: true,
//      FirstNilFirstUDR:        "\u2560\u2550",
//      FirstNilLR:              "\u2550",
//      FirstNilFirstUDLR:       "\u2550\u256c\u2550",
//      FirstNilUDLR:            "\u2550\u256a\u2550",
//      FirstNilLastUDL:         "\u2550\u2563",
//      NilFirstUDR:             "\u255f\u2500",
//      NilLR:                   "\u2500",
//      NilFirstUDLR:            "\u2500\u256b\u2500",
//      NilUDLR:                 "\u2500\u253c\u2500",
//      NilLastUDL:              "\u2500\u2562",
//      LastUR:                  "\u255a\u2550",
//      LastLR:                  "\u2550",
//      LastFirstULR:            "\u2550\u2569\u2550",
//      LastULR:                 "\u2550\u2567\u2550",
//      LastUL:                  "\u2550\u255d",
//      NilBetweenEveryRow:      true,
//  }
//
// Which will format tables like:
//
//  ╔══════════╦═════════════╤════════╤══════════╗
//  ║          ║ Bob         │ Sue    │ John     ║
//  ╠══════════╬═════════════╪════════╪══════════╣
//  ║ Hometown ║ San Antonio │ Austin │ New York ║
//  ╟──────────╫─────────────┼────────┼──────────╢
//  ║ Mother   ║ Bessie      │ Mary   │ Sarah    ║
//  ╟──────────╫─────────────┼────────┼──────────╢
//  ║ Father   ║ Rick        │ Dan    │ Mike     ║
//  ╚══════════╩═════════════╧════════╧══════════╝
func NewUnicodeBoxedAlignOptions() *AlignOptions {
	return &AlignOptions{
		FirstDR:                 "\u2554\u2550",
		FirstLR:                 "\u2550",
		FirstFirstDLR:           "\u2550\u2566\u2550",
		FirstDLR:                "\u2550\u2564\u2550",
		FirstDL:                 "\u2550\u2557",
		RowFirstUD:              "\u2551 ",
		RowSecondUD:             " \u2551 ",
		RowUD:                   " \u2502 ",
		RowLastUD:               " \u2551",
		LeaveTrailingWhitespace: true,
		FirstNilFirstUDR:        "\u2560\u2550",
		FirstNilLR:              "\u2550",
		FirstNilFirstUDLR:       "\u2550\u256c\u2550",
		FirstNilUDLR:            "\u2550\u256a\u2550",
		FirstNilLastUDL:         "\u2550\u2563",
		NilFirstUDR:             "\u255f\u2500",
		NilLR:                   "\u2500",
		NilFirstUDLR:            "\u2500\u256b\u2500",
		NilUDLR:                 "\u2500\u253c\u2500",
		NilLastUDL:              "\u2500\u2562",
		LastUR:                  "\u255a\u2550",
		LastLR:                  "\u2550",
		LastFirstULR:            "\u2550\u2569\u2550",
		LastULR:                 "\u2550\u2567\u2550",
		LastUL:                  "\u2550\u255d",
		NilBetweenEveryRow:      true,
	}
}

// Align will format a table according to options. If opts is nil,
// NewDefaultAlignOptions is used.
func Align(data [][]string, opts *AlignOptions) string {
	if data == nil || len(data) == 0 {
		return ""
	}
	if opts == nil {
		opts = NewDefaultAlignOptions()
	}
	newData := make([][]string, 0, len(data))
	for _, row := range data {
		if row == nil {
			if !opts.NilBetweenEveryRow {
				newData = append(newData, nil)
			}
			continue
		}
		if opts.Widths != nil {
			newRow := make([]string, 0, len(row))
			for col, cell := range row {
				if col >= len(opts.Widths) || opts.Widths[col] <= 0 {
					newRow = append(newRow, cell)
					continue
				}
				newRow = append(newRow, Wrap(cell, opts.Widths[col], "", ""))
			}
			row = newRow
		}
		work := make([][]string, 0, len(row))
		for _, cell := range row {
			cell = strings.Replace(cell, "\r\n", "\n", -1)
			work = append(work, strings.Split(cell, "\n"))
		}
		maxCells := 0
		for _, cells := range work {
			c := len(cells)
			if c > maxCells {
				maxCells = c
			}
		}
		newRows := make([][]string, 0)
		if opts.NilBetweenEveryRow && len(newData) != 0 {
			newData = append(newData, nil)
		}
		for c := 0; c < maxCells; c++ {
			newRow := make([]string, 0, len(work))
			for col := 0; col < len(work); col++ {
				if c < len(work[col]) {
					newRow = append(newRow, work[col][c])
				} else {
					newRow = append(newRow, "")
				}
			}
			newRows = append(newRows, newRow)
		}
		newData = append(newData, newRows...)
	}
	data = newData
	widths := make([]int, 0, len(data[0]))
	for _, row := range data {
		if row == nil {
			continue
		}
		for len(row) > len(widths) {
			widths = append(widths, len(row[len(widths)]))
		}
		for c, v := range row {
			if len(v) > widths[c] {
				widths[c] = len(v)
			}
		}
	}
	alignments := opts.Alignments
	if alignments == nil || len(alignments) < len(widths) {
		newal := append(make([]Alignment, 0, len(widths)), alignments...)
		for len(newal) < len(widths) {
			newal = append(newal, Left)
		}
		alignments = newal
	}
	est := len(opts.RowFirstUD)
	for _, w := range widths {
		est += w + len(opts.RowUD)
	}
	est += len(opts.RowLastUD) + 1
	est *= len(data)
	buf := bytes.NewBuffer(make([]byte, 0, est))
	if !AllEqual("", opts.FirstDR, opts.FirstFirstDLR, opts.FirstDLR, opts.FirstLR, opts.FirstDL) {
		buf.WriteString(opts.FirstDR)
		for col, width := range widths {
			if col == 1 {
				buf.WriteString(opts.FirstFirstDLR)
			} else if col != 0 {
				buf.WriteString(opts.FirstDLR)
			}
			for i := 0; i < width; i++ {
				buf.WriteString(opts.FirstLR)
			}
		}
		buf.WriteString(opts.FirstDL)
		buf.WriteByte('\n')
	}
	firstNil := true
	for _, row := range data {
		if row == nil {
			if firstNil {
				if !AllEqual("", opts.FirstNilFirstUDR, opts.FirstNilFirstUDLR, opts.FirstNilUDLR, opts.FirstNilLR, opts.FirstNilLastUDL) {
					buf.WriteString(opts.FirstNilFirstUDR)
					for col, width := range widths {
						if col == 1 {
							buf.WriteString(opts.FirstNilFirstUDLR)
						} else if col != 0 {
							buf.WriteString(opts.FirstNilUDLR)
						}
						for i := 0; i < width; i++ {
							buf.WriteString(opts.FirstNilLR)
						}
					}
					buf.WriteString(opts.FirstNilLastUDL)
				}
				firstNil = false
			} else {
				if !AllEqual("", opts.NilFirstUDR, opts.NilFirstUDLR, opts.NilUDLR, opts.NilLR, opts.NilLastUDL) {
					buf.WriteString(opts.NilFirstUDR)
					for col, width := range widths {
						if col == 1 {
							buf.WriteString(opts.NilFirstUDLR)
						} else if col != 0 {
							buf.WriteString(opts.NilUDLR)
						}
						for i := 0; i < width; i++ {
							buf.WriteString(opts.NilLR)
						}
					}
					buf.WriteString(opts.NilLastUDL)
				}
			}
			buf.WriteByte('\n')
			continue
		}
		buf.WriteString(opts.RowFirstUD)
		for c, v := range row {
			if c == 1 {
				buf.WriteString(opts.RowSecondUD)
			} else if c != 0 {
				buf.WriteString(opts.RowUD)
			}
			switch alignments[c] {
			case Right:
				for i := widths[c] - len(v); i > 0; i-- {
					buf.WriteRune(' ')
				}
				buf.WriteString(v)
			case Center:
				for i := (widths[c] - len(v)) / 2; i > 0; i-- {
					buf.WriteRune(' ')
				}
				buf.WriteString(v)
				if opts.LeaveTrailingWhitespace || c < len(row)-1 {
					for i := widths[c] - ((widths[c]-len(v))/2 + len(v)); i > 0; i-- {
						buf.WriteRune(' ')
					}
				}
			default:
				buf.WriteString(v)
				if opts.LeaveTrailingWhitespace || c < len(row)-1 {
					for i := widths[c] - len(v); i > 0; i-- {
						buf.WriteRune(' ')
					}
				}
			}
		}
		buf.WriteString(opts.RowLastUD)
		buf.WriteByte('\n')
	}
	if !AllEqual("", opts.LastUR, opts.LastFirstULR, opts.LastULR, opts.LastLR, opts.LastUL) {
		buf.WriteString(opts.LastUR)
		for col, width := range widths {
			if col == 1 {
				buf.WriteString(opts.LastFirstULR)
			} else if col != 0 {
				buf.WriteString(opts.LastULR)
			}
			for i := 0; i < width; i++ {
				buf.WriteString(opts.LastLR)
			}
		}
		buf.WriteString(opts.LastUL)
		buf.WriteByte('\n')
	}
	return buf.String()
}

// Sentence converts the value into a sentence, uppercasing the first character
// and ensuring the string ends with a period. Useful to output better looking
// error.Error() messages, which are all lower case with no trailing period by
// convention.
func Sentence(value string) string {
	if value != "" {
		if value[len(value)-1] != '.' {
			value = strings.ToUpper(value[:1]) + value[1:] + "."
		} else {
			value = strings.ToUpper(value[:1]) + value[1:]
		}
	}
	return value
}

// StringSliceToLowerSort provides a sort.Interface that will sort a []string
// by their strings.ToLower values. This isn't exactly a case insensitive sort
// due to Unicode situations, but is usually good enough.
type StringSliceToLowerSort []string

func (s StringSliceToLowerSort) Len() int {
	return len(s)
}

func (s StringSliceToLowerSort) Swap(x int, y int) {
	s[x], s[y] = s[y], s[x]
}

func (s StringSliceToLowerSort) Less(x int, y int) bool {
	return strings.ToLower(s[x]) < strings.ToLower(s[y])
}

// Wrap wraps text for more readable output.
func Wrap(text string, width int, indent1 string, indent2 string) string {
	if width < 1 {
		width = GetTTYWidth() + width
	}
	bs := []byte(text)
	bs = wrap(bs, width, []byte(indent1), []byte(indent2))
	return string(bytes.Trim(bs, "\n"))
}

func wrap(text []byte, width int, indent1 []byte, indent2 []byte) []byte {
	if len(text) == 0 {
		return text
	}
	text = bytes.Replace(text, []byte{'\r', '\n'}, []byte{'\n'}, -1)
	var out bytes.Buffer
	for _, par := range bytes.Split([]byte(text), []byte{'\n', '\n'}) {
		par = bytes.Replace(par, []byte{'\n'}, []byte{' '}, -1)
		lineLen := 0
		start := true
		for _, word := range bytes.Split(par, []byte{' '}) {
			wordLen := len(word)
			if wordLen == 0 {
				continue
			}
			scan := word
			for len(scan) > 1 {
				i := bytes.IndexByte(scan, '\x1b')
				if i == -1 {
					break
				}
				j := bytes.IndexByte(scan[i+1:], 'm')
				if j == -1 {
					i++
				} else {
					j += 2
					wordLen -= j
					scan = scan[i+j:]
				}
			}
			if start {
				out.Write(indent1)
				lineLen += len(indent1)
				out.Write(word)
				lineLen += wordLen
				start = false
			} else if lineLen+1+wordLen > width {
				out.WriteByte('\n')
				out.Write(indent2)
				out.Write(word)
				lineLen = len(indent2) + wordLen
			} else {
				out.WriteByte(' ')
				out.Write(word)
				lineLen += 1 + wordLen
			}
		}
		out.WriteByte('\n')
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// AllEqual returns true if all the values each equal the compare string.
func AllEqual(compare string, values ...string) bool {
	for _, v := range values {
		if v != compare {
			return false
		}
	}
	return true
}
