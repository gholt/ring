package main

import (
	"fmt"
	"os"

	"github.com/gholt/brimtext"
	"github.com/gholt/ring"
)

func main() {
	i := -1
	for j, v := range os.Args {
		if v == "--no-color" {
			i = j
			break
		}
	}
	if i != -1 {
		os.Args = append(os.Args[:i], os.Args[i+1:]...)
	}
	if err := ring.CLI(os.Args, os.Stdout, i == -1); err != nil {
		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
		os.Exit(1)
	}
}
