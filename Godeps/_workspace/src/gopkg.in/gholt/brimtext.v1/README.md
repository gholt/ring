# BrimText
## Text Tools for Go

Package brimtext contains tools for working with text. Probably the most
complex of these tools is Align, which allows for formatting "pretty tables".

[API Documentation](http://godoc.org/gopkg.in/gholt/brimtext.v1)

This is the latest stable version of the package.  
For the latest development version of the package, switch to the
[master branch](https://github.com/gholt/brimtext)  
or use `github.com/gholt/brimtext` as the import path.  
Also, you'd want to use the
[Dev API Documentation](http://godoc.org/github.com/gholt/brimtext).

> Copyright Gregory Holt. All rights reserved.  
> Use of this source code is governed by a BSD-style  
> license that can be found in the LICENSE file.

## Example Code

```go
package main

import (
    "fmt"

    "gopkg.in/gholt/brimtext.v1"
)

func main() {
    example := 1
    fmt.Printf("This is the %d%s example:\n\n",
        example, brimtext.OrdinalSuffix(example))
    fmt.Println(brimtext.Align([][]string{
        []string{"", "Bob", "Sue", "John"},
        []string{"Hometown", "San Antonio", "Austin", "New York"},
        []string{"Mother", "Bessie", "Mary", "Sarah"},
        []string{"Father", "Rick", "Dan", "Mike"},
    }, brimtext.NewDefaultAlignOptions()))

    example++
    fmt.Printf("This is the %d%s example:\n\n",
        example, brimtext.OrdinalSuffix(example))
    fmt.Println(brimtext.Align([][]string{
        []string{"", "Bob", "Sue", "John"},
        nil,
        []string{"Hometown", "San Antonio", "Austin", "New York"},
        []string{"Mother", "Bessie", "Mary", "Sarah"},
        []string{"Father", "Rick", "Dan", "Mike"},
    }, brimtext.NewSimpleAlignOptions()))

    example++
    fmt.Printf("This is the %d%s example:\n\n",
        example, brimtext.OrdinalSuffix(example))
    data := [][]int{
        {8, 20, 11},
        {5, 11, 10},
        {3, 9, 1},
        {1200000, 2400000, 1700000},
    }
    table := [][]string{[]string{"", "Bob", "Sue", "John"}}
    for rowNum, values := range data {
        row := []string{""}
        prefix := ""
        switch rowNum {
        case 0:
            row[0] = "Shot Attempts"
        case 1:
            row[0] = "Shots Made"
        case 2:
            row[0] = "Shots Missed"
        case 3:
            row[0] = "Salary"
            prefix = "$"
        }
        for _, v := range values {
            row = append(row, prefix+brimtext.ThousandsSep(int64(v), ","))
        }
        table = append(table, row)
    }
    opts := brimtext.NewUnicodeBoxedAlignOptions()
    opts.Alignments = []brimtext.Alignment{
        brimtext.Right,
        brimtext.Right,
        brimtext.Right,
        brimtext.Right,
    }
    fmt.Println(brimtext.Align(table, opts))
}
```

## Example Output

```
This is the 1st example:

         Bob         Sue    John
Hometown San Antonio Austin New York
Mother   Bessie      Mary   Sarah
Father   Rick        Dan    Mike

This is the 2nd example:

+----------+-------------+--------+----------+
|          | Bob         | Sue    | John     |
+----------+-------------+--------+----------+
| Hometown | San Antonio | Austin | New York |
| Mother   | Bessie      | Mary   | Sarah    |
| Father   | Rick        | Dan    | Mike     |
+----------+-------------+--------+----------+

This is the 3rd example:

╔═══════════════╦════════════╤════════════╤════════════╗
║               ║        Bob │        Sue │       John ║
╠═══════════════╬════════════╪════════════╪════════════╣
║ Shot Attempts ║          8 │         20 │         11 ║
╟───────────────╫────────────┼────────────┼────────────╢
║    Shots Made ║          5 │         11 │         10 ║
╟───────────────╫────────────┼────────────┼────────────╢
║  Shots Missed ║          3 │          9 │          1 ║
╟───────────────╫────────────┼────────────┼────────────╢
║        Salary ║ $1,200,000 │ $2,400,000 │ $1,700,000 ║
╚═══════════════╩════════════╧════════════╧════════════╝
```
