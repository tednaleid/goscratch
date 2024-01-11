package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	// Use bufio.NewReader to read from stdin
	reader := bufio.NewReader(os.Stdin)

	// Read the entire input from stdin
	input, err := reader.ReadString(0)
	if err != nil && err.Error() != "EOF" {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		os.Exit(1)
	}

	// Unmarshal the JSON data into a map
	var data map[string]json.RawMessage
	err = json.Unmarshal([]byte(input), &data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// Iterate over the map and calculate sizes
	for key, value := range data {
		size := len(value)
		fmt.Printf("%s: %d bytes\n", key, size)
	}
}
