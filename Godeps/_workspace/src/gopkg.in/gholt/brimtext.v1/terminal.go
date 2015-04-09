// Copyright Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package brimtext

import (
	"os"

	"golang.org/x/crypto/ssh/terminal"
)

// GetTTYWidth returns the width of controlling TTY if it can or 80.
func GetTTYWidth() int {
	var tty *os.File
	var err error
	if tty, err = os.OpenFile("/dev/tty", os.O_RDWR, 0600); err != nil {
		tty = os.Stdout
	} else {
		defer tty.Close()
	}
	if width, _, err := terminal.GetSize(int(tty.Fd())); err != nil {
		return 80
	} else {
		return width - 1
	}
}
