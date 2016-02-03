package main

import (
	"fmt"
	"os"

	"github.com/gholt/ring"
	"gopkg.in/gholt/brimtext.v1"
)

func main() {
	if err := ring.CLI(os.Args, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
		os.Exit(1)
	}
}
