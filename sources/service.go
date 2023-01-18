package sources

import (
	"encoding/csv"
	"errors"
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
func readFile(filenameInput string, chunkSize uint16) ([]string, [][][]string, error) {

	f, err := os.Open(filenameInput)
	if err != nil {
		return nil, nil, errors.New("failed to open file")
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
			errF := f.Close()
			if errF != nil {
				return nil, nil, errors.New("failed closing file")
			}
			return nil, nil, errors.New("failed reading a row")
		}
		if contains(row, "") {
			continue
		}
		if len(header) == 0 {
			header = row
			continue
		}
		if len(chunk) == int(chunkSize) {
			chunks = append(chunks, chunk)
			chunk = nil
		}
		chunk = append(chunk, row)
	}

	err = f.Close()
	if err != nil {
		return nil, nil, errors.New("failed closing file")
	}
	return header, chunks, nil
}

func loadFiles(header []string, chunks [][][]string) error {
	chunkFilename := "resources/chunk"

	for i, chunk := range chunks {
		currentChunkFilename := chunkFilename + strconv.Itoa(i) + ".csv"

		f, err := os.Create(currentChunkFilename)
		if err != nil {
			return errors.New("failed creating a file")
		}

		writer := csv.NewWriter(f)
		err = writer.Write(header)
		if err != nil {
			errF := f.Close()
			if errF != nil {
				return errors.New("failed closing a file")
			}
			return errors.New("failed writing row to file")
		}
		writer.Flush()

		err = writer.WriteAll(chunk)
		if err != nil {
			errF := f.Close()
			if errF != nil {
				return errors.New("failed closing a file")
			}
			return errors.New("failed writing rows to file")
		}

		err = f.Close()
		if err != nil {
			return errors.New("failed closing a file")
		}
	}

	return nil
}

// Extract reads from an input .csv file, it extracts all the lines that have all the fields completed
// and organizes them in chunks of dimension chunkSize and exports each chunk in separate files
// (chunkX.csv where X is the id of the chunk)
func Extract(filenameInput string, chunkSize uint16) error {
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
