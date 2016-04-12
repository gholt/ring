package main

import (
	"fmt"
	"os"

	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
	"golang.org/x/crypto/ssh/terminal"
)

func main() {
	var args []string
	color := terminal.IsTerminal(int(os.Stdout.Fd()))
	for _, arg := range os.Args {
		switch arg {
		case "--color":
			color = true
		case "--no-color":
			color = false
		default:
			args = append(args, arg)
		}
	}
	if err := ring.CLI(args, os.Stdout, color); err != nil {
		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
		os.Exit(1)
	}
}
