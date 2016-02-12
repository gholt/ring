package main

import (
	"fmt"
	"os"

	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
)

func main() {
	if err := ring.CLI(os.Args, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
		os.Exit(1)
	}
}
