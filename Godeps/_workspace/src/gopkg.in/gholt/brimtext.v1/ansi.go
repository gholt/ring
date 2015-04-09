// Copyright Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package brimtext

import (
	"bytes"
	"strconv"
	"strings"
)

// ANSIEscapeCodes is the defining structure for the more commonly used
// ANSIEscape global variable.
type ANSIEscapeCodes struct {
	Reset                                                         []byte
	Bold                                                          []byte
	BBlack, BRed, BGreen, BYellow, BBlue, BMagenta, BCyan, BWhite []byte
	FBlack, FRed, FGreen, FYellow, FBlue, FMagenta, FCyan, FWhite []byte
}

// ANSIEscape provides ease of access to common ANSI Escape Codes.
var ANSIEscape = ANSIEscapeCodes{
	Reset:    []byte{27, '[', '0', 'm'},
	Bold:     []byte{27, '[', '1', 'm'},
	BBlack:   []byte{27, '[', '4', '0', 'm'},
	BRed:     []byte{27, '[', '4', '1', 'm'},
	BGreen:   []byte{27, '[', '4', '2', 'm'},
	BYellow:  []byte{27, '[', '4', '3', 'm'},
	BBlue:    []byte{27, '[', '4', '4', 'm'},
	BMagenta: []byte{27, '[', '4', '5', 'm'},
	BCyan:    []byte{27, '[', '4', '6', 'm'},
	BWhite:   []byte{27, '[', '4', '7', 'm'},
	FBlack:   []byte{27, '[', '3', '0', 'm'},
	FRed:     []byte{27, '[', '3', '1', 'm'},
	FGreen:   []byte{27, '[', '3', '2', 'm'},
	FYellow:  []byte{27, '[', '3', '3', 'm'},
	FBlue:    []byte{27, '[', '3', '4', 'm'},
	FMagenta: []byte{27, '[', '3', '5', 'm'},
	FCyan:    []byte{27, '[', '3', '6', 'm'},
	FWhite:   []byte{27, '[', '3', '7', 'm'},
}

// ClosestANSIForegroundString translates the CSS-style color (e.g. "#ac8"
// "#ffee66") string to the closest ANSIEscape sequence for that foreground
// color.
func ClosestANSIForegroundString(value string) []byte {
	value = strings.ToLower(value)
	if value == "" {
		return []byte{}
	} else if value[0] == '#' {
		value = value[1:]
	}
	if len(value) == 3 {
		red, _ := strconv.ParseInt(value[:1], 16, 0)
		green, _ := strconv.ParseInt(value[1:2], 16, 0)
		blue, _ := strconv.ParseInt(value[2:], 16, 0)
		red <<= 4
		green <<= 4
		blue <<= 4
		return ClosestANSIForeground(int(red), int(green), int(blue))
	} else if len(value) == 6 {
		red, _ := strconv.ParseInt(value[:2], 16, 0)
		green, _ := strconv.ParseInt(value[2:4], 16, 0)
		blue, _ := strconv.ParseInt(value[4:], 16, 0)
		return ClosestANSIForeground(int(red), int(green), int(blue))
	} else {
		return []byte{}
	}
}

// ClosestANSIForeground translates the RGB values to the closest ANSIEscape
// sequence for that foreground color.
func ClosestANSIForeground(red int, green int, blue int) []byte {
	var sequence bytes.Buffer
	if red < 96 {
		red = 0
	} else if red < 175 {
		red = 1
	} else {
		red = 2
	}
	if green < 96 {
		green = 0
	} else if green < 175 {
		green = 1
	} else {
		green = 2
	}
	if blue < 96 {
		blue = 0
	} else if blue < 175 {
		blue = 1
	} else {
		blue = 2
	}
	if red == 2 || green == 2 || blue == 2 {
		sequence.Write(ANSIEscape.Bold)
	}
	if red == 0 {
		if green == 0 {
			if blue == 0 {
				sequence.Write(ANSIEscape.FBlack)
			} else {
				sequence.Write(ANSIEscape.FBlue)
			}
		} else {
			if blue == 0 {
				sequence.Write(ANSIEscape.FGreen)
			} else {
				sequence.Write(ANSIEscape.FCyan)
			}
		}
	} else if green == 0 {
		if blue == 0 {
			sequence.Write(ANSIEscape.FRed)
		} else {
			sequence.Write(ANSIEscape.FMagenta)
		}
	} else {
		if blue == 0 {
			sequence.Write(ANSIEscape.FYellow)
		} else {
			sequence.Write(ANSIEscape.FWhite)
		}
	}
	return sequence.Bytes()
}
