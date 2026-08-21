package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"freebuff-reverse/internal/channels/freebuff"
)

func main() {
	inputPath := flag.String("input", "", "path to simulation JSON; use '-' for stdin; omitted uses the built-in sample")
	flag.Parse()

	var input freebuff.SchedulerSimulationInput
	if *inputPath == "" {
		input = freebuff.DefaultSchedulerSimulationInput()
	} else {
		reader, closeInput, err := inputReader(*inputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "freebuff-scheduler-sim: %v\n", err)
			os.Exit(1)
		}
		defer closeInput()
		if err := json.NewDecoder(reader).Decode(&input); err != nil {
			fmt.Fprintf(os.Stderr, "freebuff-scheduler-sim: decode input: %v\n", err)
			os.Exit(1)
		}
	}
	report, err := freebuff.RunSchedulerSimulation(context.Background(), input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "freebuff-scheduler-sim: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "freebuff-scheduler-sim: encode output: %v\n", err)
		os.Exit(1)
	}
}

func inputReader(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	return file, func() { _ = file.Close() }, nil
}
