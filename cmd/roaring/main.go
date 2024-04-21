package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/RoaringBitmap/roaring"
)

func main() {
	// the max value of a 32-bit unsigned integer is 4294967295
	// so to store all possible 10-digit numbers, we'll need 3 roaring bitmaps
	// for numbers 0-4294967295
	rb0 := roaring.New()
	// for numbers 4294967296-8589934591
	rb4294967296 := roaring.New()
	// for numbers 8589934592-9999999999
	rb8589934592 := roaring.New()

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		// convert to a 64-bit unsigned integer
		num, err := strconv.ParseUint(scanner.Text(), 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting '%s' to integer: %v\n", scanner.Text(), err)
			continue
		}

		if num <= 4294967295 {
			rb0.Add(uint32(num))
		} else if num <= 8589934591 {
			// need to subtract 4294967296 to fit in a 32-bit integer
			rb4294967296.Add(uint32(num - 4294967296))
		} else if num <= 9999999999 {
			// need to subtract 8589934592 to fit in a 32-bit integer
			rb8589934592.Add(uint32(num - 8589934592))
		} else {
			fmt.Fprintf(os.Stderr, "Error: integer '%d' is larger than 10-digits\n", num)
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading standard input: %v\n", err)
		os.Exit(1)
	}

	bitmaps := []*roaring.Bitmap{rb0, rb4294967296, rb8589934592}

	for i, rb := range bitmaps {
		memorySize := rb.GetSizeInBytes()
		cardinality := rb.GetCardinality()

		fmt.Printf("Memory size of bitmap %d: %d bytes, %.2fMB\n", i, memorySize, float64(memorySize)/1024/1024)
		fmt.Printf("Cardinality of bitmap %d: %d\n", i, cardinality)
	}

	memorySize := rb0.GetSizeInBytes() + rb4294967296.GetSizeInBytes() + rb8589934592.GetSizeInBytes()
	cardinality := rb0.GetCardinality() + rb4294967296.GetCardinality() + rb8589934592.GetCardinality()

	fmt.Printf("Total memory size: %d bytes, %.2fMB\n", memorySize, float64(memorySize)/1024/1024)
	fmt.Printf("Cardinality: %d\n", cardinality)
}
