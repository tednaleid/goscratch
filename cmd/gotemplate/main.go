package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	templateFlag := flag.String("d", "", "Template string")
	flag.Parse()

	if *templateFlag == "" {
		fmt.Println("Error: Template must be provided using the -d flag")
		os.Exit(1)
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		result := applyTemplate(*templateFlag, fields)
		fmt.Println(result)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading standard input: %v\n", err)
		os.Exit(1)
	}
}

func applyTemplate(template string, values []string) string {
	var args []interface{}
	re := regexp.MustCompile(`%[-+]?[0-9]*\.?[0-9]*[bcdeEfFgGopqstvxX]`)
	matches := re.FindAllStringIndex(template, -1)

	for i, match := range matches {
		if i >= len(values) {
			break
		}
		placeholder := template[match[0]:match[1]]
		value := values[i]

		switch placeholder[len(placeholder)-1] {
		case 'd', 'b', 'c', 'o', 'x', 'X':
			intVal, err := strconv.ParseInt(value, 0, 64)
			if err != nil {
				intVal = 0
			}
			args = append(args, intVal)
		case 'e', 'E', 'f', 'F', 'g', 'G':
			floatVal, err := strconv.ParseFloat(value, 64)
			if err != nil {
				floatVal = 0.0
			}
			args = append(args, floatVal)
		case 't':
			boolVal, err := strconv.ParseBool(value)
			if err != nil {
				boolVal = false
			}
			args = append(args, boolVal)
		case 'v':
			if val, err := strconv.ParseInt(value, 0, 64); err == nil {
				args = append(args, val)
			} else if val, err := strconv.ParseFloat(value, 64); err == nil {
				args = append(args, val)
			} else if val, err := strconv.ParseBool(value); err == nil {
				args = append(args, val)
			} else {
				args = append(args, value)
			}
		default:
			args = append(args, value)
		}
	}

	return fmt.Sprintf(template, args...)
}
