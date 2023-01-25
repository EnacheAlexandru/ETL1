package fprocessing

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

func contains[T comparable](arr []T, elem T) bool {
	for _, t := range arr {
		if t == elem {
			return true
		}
	}

	return false
}

func isEOF(err error, chunks *[][][]string, chunk [][]string) bool {
	if err == io.EOF {
		*chunks = append(*chunks, chunk)
		return true
	}

	return false
}

func isHeaderRead(header *[]string, row []string) bool {
	if len(*header) > 0 {
		return true
	}
	*header = row

	return false
}

func chunkFull(chunks *[][][]string, chunk *[][]string, row []string, chunkSize int) {
	if len(*chunk) == chunkSize {
		*chunks = append(*chunks, *chunk)
		*chunk = nil
	}

	*chunk = append(*chunk, row)
}

// return values
// the header (a list of fields)
// the list of chunks; a chunk is a list of rows; a row is a list of fields
// the first error that occurred
func readFile(filenameInput string, chunkSize int) ([]string, [][][]string, error) {
	f, err := os.Open(filenameInput)
	if err != nil {
		return nil, nil, fmt.Errorf("failed opening input file")
	}

	var header []string
	var chunks [][][]string
	var chunk [][]string

	reader := csv.NewReader(f)

	for {
		row, err := reader.Read()

		if isEOF(err, &chunks, chunk) == true {
			break
		}
		if err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("failed reading input file")
		}
		if contains(row, "") {
			continue
		}
		if isHeaderRead(&header, row) == false {
			continue
		}
		chunkFull(&chunks, &chunk, row, chunkSize)
	}

	if f.Close() != nil {
		return nil, nil, fmt.Errorf("failed closing input file")
	}

	return header, chunks, nil
}

func writeRowCSV(f *os.File, writer *csv.Writer, row []string) error {
	if writer.Write(row) != nil {
		_ = f.Close()
		return fmt.Errorf("failed writing row to output file")
	}
	writer.Flush()

	return nil
}

func writeRowsCSV(f *os.File, writer *csv.Writer, rows [][]string) error {
	if writer.WriteAll(rows) != nil {
		_ = f.Close()
		return fmt.Errorf("failed writing rows to output file")
	}

	return nil
}

func writeFiles(header []string, chunks [][][]string, chunkFilename string) error {
	for i, chunk := range chunks {
		currentChunkFilename := chunkFilename + strconv.Itoa(i) + ".csv"

		f, err := os.Create(currentChunkFilename)
		if err != nil {
			return fmt.Errorf("failed creating output file")
		}

		writer := csv.NewWriter(f)

		if writeRowCSV(f, writer, header) != nil {
			return err
		}

		if writeRowsCSV(f, writer, chunk) != nil {
			return err
		}

		if f.Close() != nil {
			return fmt.Errorf("failed closing output file")
		}
	}

	return nil
}

// Extract reads from an input .csv file, it extracts all the lines that have all the fields completed
// and organizes them in chunks of dimension chunkSize and exports each chunk in separate files
// (chunkX.csv where X is the id of the chunk)
func Extract(filenameInput, chunkFilename string, chunkSize int) error {
	if chunkSize < 1 {
		return fmt.Errorf("chunk size should be at least 1")
	}

	header, chunks, err := readFile(filenameInput, chunkSize)
	if err != nil {
		return err
	}

	if writeFiles(header, chunks, chunkFilename) != nil {
		return err
	}

	return nil
}
