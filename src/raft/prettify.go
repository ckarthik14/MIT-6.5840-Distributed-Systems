package raft

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	columnWidth = 25
	// Adjust this if you have more or fewer servers
	totalServers = 5
)

func prettify() {
	inputFileName := "debug.txt"
	outputFileName := "debug_pretty.txt"

	// Open the input file
	inFile, err := os.Open(inputFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer inFile.Close()

	// Create (or overwrite) the output file
	outFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	// Read the input file line by line
	reader := bufio.NewReader(inFile)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading line: %v\n", err)
			os.Exit(1)
		}

		// Trim whitespace and skip empty lines
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Split fields. Expect [0] -> timestamp, [1] -> server (e.g. S0), [2] -> LOG-level, [3..end] -> message
		parts := strings.Fields(line)
		if len(parts) < 4 {
			// Not enough parts, skip
			continue
		}

		serverName := parts[1]
		message := strings.Join(parts[3:], " ")

		// Parse the server index from the name, e.g. "S0" -> 0, "S1" -> 1, etc.
		colIndex := parseServerIndex(serverName)
		if colIndex < 0 || colIndex >= totalServers {
			// If you encounter a server outside the range, decide whether to skip or handle differently
			fmt.Fprintf(os.Stderr, "Warning: server index out of range for %q\n", serverName)
			continue
		}

		// Wrap the message to 25 characters
		chunks := wrapMessage(message, columnWidth)

		// Print each wrapped chunk in the correct column
		for _, chunk := range chunks {
			// Prepare a row with empty columns for all servers
			columns := make([]string, totalServers)

			// Assign the chunk to the proper column
			columns[colIndex] = chunk

			// Format each column to exactly 25 characters (left-aligned)
			for i, c := range columns {
				columns[i] = fmt.Sprintf("%-25s", c)
			}

			// Print the line (joined columns)
			fmt.Fprintln(writer, strings.Join(columns, ""))
		}

		// Blank line after each log entry (but not between wrapped chunks)
		fmt.Fprintln(writer)
	}
}

// parseServerIndex extracts the integer part from a server name like "S0", "S1".
// Returns -1 if parsing fails.
func parseServerIndex(serverName string) int {
	// e.g. serverName = "S0", "S1", ...
	// We assume the first character is 'S' and the rest is an integer.
	if len(serverName) < 2 || serverName[0] != 'S' {
		return -1
	}
	indexStr := serverName[1:]
	idx, err := strconv.Atoi(indexStr)
	if err != nil {
		return -1
	}
	return idx
}

// wrapMessage splits the message into slices of up to `width` runes each
func wrapMessage(msg string, width int) []string {
	runes := []rune(msg)
	if len(runes) == 0 {
		return []string{""}
	}

	var chunks []string
	for len(runes) > 0 {
		if len(runes) <= width {
			chunks = append(chunks, string(runes))
			break
		}
		chunks = append(chunks, string(runes[:width]))
		runes = runes[width:]
	}
	return chunks
}
