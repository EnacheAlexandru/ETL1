package sources

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

func contains[T comparable](s []T, e T) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}

// return values
// the header (a list of fields)
// the list of chunks; a chunk is a list of rows; a row is a list of fields
// the first error that occurred
func readFile(filenameInput string, chunkSize int) ([]string, [][][]string, error) {
	f, err := os.Open(filenameInput)
	if err != nil {
		return nil, nil, err
	}

	var header []string
	var chunks [][][]string

	var chunk [][]string
	reader := csv.NewReader(f)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			chunks = append(chunks, chunk)
			break
		}
		if err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		if contains(row, "") {
			continue
		}
		if len(header) == 0 {
			header = row
			continue
		}
		if len(chunk) == chunkSize {
			chunks = append(chunks, chunk)
			chunk = nil
		}
		chunk = append(chunk, row)
	}

	err = f.Close()
	if err != nil {
		return nil, nil, err
	}
	return header, chunks, nil
}

func loadFiles(header []string, chunks [][][]string) error {
	chunkFilename := "resources/chunk"

	for i, chunk := range chunks {
		currentChunkFilename := chunkFilename + strconv.Itoa(i) + ".csv"

		f, err := os.Create(currentChunkFilename)
		if err != nil {
			return err
		}

		writer := csv.NewWriter(f)
		err = writer.Write(header)
		if err != nil {
			_ = f.Close()
			return err
		}
		writer.Flush()

		err = writer.WriteAll(chunk)
		if err != nil {
			_ = f.Close()
			return err
		}

		err = f.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

// Extract reads from an input .csv file, it extracts all the lines that have all the fields completed
// and organizes them in chunks of dimension chunkSize and exports each chunk in separate files
// (chunkX.csv where X is the id of the chunk)
func Extract(filenameInput string, chunkSize int) error {
	if chunkSize < 2 {
		return fmt.Errorf("chunk size should be at least 2")
	}

	header, chunks, err := readFile(filenameInput, chunkSize)
	if err != nil {
		return err
	}

	err = loadFiles(header, chunks)
	if err != nil {
		return err
	}

	return nil
}
