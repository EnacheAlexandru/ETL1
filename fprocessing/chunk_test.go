package fprocessing

import (
	"encoding/csv"
	"os"
	"testing"
)

func areSlicesEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

func areSlicesOfSlicesEqual[T comparable](a, b [][]T) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if areSlicesEqual(v, b[i]) == false {
			return false
		}
	}

	return true
}

func areSlicesOfSlicesOfSlicesEqual[T comparable](a, b [][][]T) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if areSlicesOfSlicesEqual(v, b[i]) == false {
			return false
		}
	}

	return true
}

// checks if an element is present in given array
func TestContains(t *testing.T) {
	arr := []int{1, 2, 3, 4, 5}
	elem := 4

	want := true
	got := contains(arr, elem)
	if got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}
}

// checks if an element is not present in given array
func TestContainsNotFound(t *testing.T) {
	arr := []int{1, 2, 3, 4, 5}
	elem := 6

	want := false
	got := contains(arr, elem)
	if got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}
}

// if the header is empty, the new row will become the header
func TestIsHeaderReadEmpty(t *testing.T) {
	var header []string
	row := []string{"id", "brand", "model"}

	want := false
	got := isHeaderRead(&header, row)
	if got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}

	wantHeader := true
	gotHeader := areSlicesEqual(header, row)
	if gotHeader != wantHeader {
		t.Errorf("got: %v, want: %v", gotHeader, wantHeader)
	}
}

// if the header is already assigned, any new row will be ignored
func TestIsHeaderReadNotEmpty(t *testing.T) {
	header := []string{"id", "brand", "model"}
	row := []string{"8", "bmw", "e46"}

	want := true
	got := isHeaderRead(&header, row)
	if got != want {
		t.Errorf("got: %v, want: %v", got, want)
	}
}

// if the chunkSize isn't reached, the new row will be added to the current chunk
func TestChunkFullNot(t *testing.T) {
	var chunks [][][]string
	row := []string{"8", "vw", "passat"}
	chunkSize := 3

	gotChunk := [][]string{{"6", "bmw", "e46"}, {"7", "skoda", "octavia"}}
	wantChunk := [][]string{{"6", "bmw", "e46"}, {"7", "skoda", "octavia"}, {"8", "vw", "passat"}}

	chunkFull(&chunks, &gotChunk, row, chunkSize)

	if !areSlicesOfSlicesEqual(gotChunk, wantChunk) {
		t.Errorf("got: %v, want: %v", gotChunk, wantChunk)
	}

	wantChunksLen := 0
	gotChunksLen := len(chunks)
	if wantChunksLen != gotChunksLen {
		t.Errorf("got: %v, want: %v", gotChunksLen, wantChunksLen)
	}
}

// if the chunkSize is reached, the current chunk will be added to the list of chunks
// and the new row will be added to a new chunk
func TestChunkFull(t *testing.T) {
	row := []string{"8", "vw", "passat"}
	chunkSize := 2
	gotChunk := [][]string{{"6", "bmw", "e46"}, {"7", "skoda", "octavia"}}

	var gotChunks [][][]string
	wantChunks := [][][]string{{{"6", "bmw", "e46"}, {"7", "skoda", "octavia"}}}

	chunkFull(&gotChunks, &gotChunk, row, chunkSize)

	if !areSlicesOfSlicesOfSlicesEqual(gotChunks, wantChunks) {
		t.Errorf("got: %v, want: %v", gotChunks, wantChunks)
	}

	wantChunk := [][]string{{"8", "vw", "passat"}}
	if !areSlicesOfSlicesEqual(gotChunk, wantChunk) {
		t.Errorf("got: %v, want: %v", gotChunk, wantChunk)
	}
}

// if the legal rows cannot be divided by the chunkSize,
// all chunks except the last one, should have the size of chunkSize
func TestReadFileAllValidLastChunkNotFull(t *testing.T) {
	filename := "data/test/inputreadfileallvalidlastchunknotfull_test.csv"
	chunkSize := 2

	gotHeader, gotChunks, gotErr := readFile(filename, chunkSize)

	if gotErr != nil {
		t.Errorf("got: %v, want: %v", gotErr, nil)
	}

	wantChunks := [][][]string{
		{
			{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
			{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
		},
		{
			{"3", "Gerri", "Choffin", "gchoffin2@ning.com", "Male", "9.254.198.50"},
			{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
		},
		{
			{"5", "Benoite", "Calve", "bjaffray4@github.com", "Female", "148.75.193.241"},
		},
	}

	if !areSlicesOfSlicesOfSlicesEqual(gotChunks, wantChunks) {
		t.Errorf("got: %v, want: %v", gotChunks, wantChunks)
	}

	wantHeader := []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}

	if !areSlicesEqual(gotHeader, wantHeader) {
		t.Errorf("got: %v, want: %v", gotHeader, wantHeader)
	}
}

// if the legal rows can be divided by the chunkSize,
// all chunks should have the size of chunkSize
func TestReadFileAllValidChunksFull(t *testing.T) {
	filename := "data/test/inputreadfileallvalidchunksfull_test.csv"
	chunkSize := 2

	gotHeader, gotChunks, gotErr := readFile(filename, chunkSize)

	if gotErr != nil {
		t.Errorf("got: %v, want: %v", gotErr, nil)
	}

	wantChunks := [][][]string{
		{
			{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
			{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
		},
		{
			{"3", "Gerri", "Choffin", "gchoffin2@ning.com", "Male", "9.254.198.50"},
			{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
		},
	}

	if !areSlicesOfSlicesOfSlicesEqual(gotChunks, wantChunks) {
		t.Errorf("got: %v, want: %v", gotChunks, wantChunks)
	}

	wantHeader := []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}

	if !areSlicesEqual(gotHeader, wantHeader) {
		t.Errorf("got: %v, want: %v", gotHeader, wantHeader)
	}
}

// the invalid rows should be ignored
func TestReadFileSomeInvalidRows(t *testing.T) {
	filename := "data/test/inputreadfilesomeinvalidrows_test.csv"
	chunkSize := 2

	gotHeader, gotChunks, gotErr := readFile(filename, chunkSize)

	if gotErr != nil {
		t.Errorf("got: %v, want: %v", gotErr, nil)
	}

	wantChunks := [][][]string{
		{
			{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
			{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
		},
		{
			{"5", "Benoite", "Calve", "bjaffray4@github.com", "Female", "148.75.193.241"},
		},
	}

	if !areSlicesOfSlicesOfSlicesEqual(gotChunks, wantChunks) {
		t.Errorf("got: %v, want: %v", gotChunks, wantChunks)
	}

	wantHeader := []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}

	if !areSlicesEqual(gotHeader, wantHeader) {
		t.Errorf("got: %v, want: %v", gotHeader, wantHeader)
	}
}

// if there is an error opening the input file
func TestReadFileOpenError(t *testing.T) {
	filename := "data/test/error.csv"
	chunkSize := 2

	gotHeader, gotChunks, gotErr := readFile(filename, chunkSize)

	wantErr := "failed opening input file"
	if gotErr.Error() != wantErr {
		t.Errorf("got: %v, want: %v", gotErr.Error(), wantErr)
	}

	if gotChunks != nil {
		t.Errorf("got: %v, want: %v", gotChunks, nil)
	}

	if gotHeader != nil {
		t.Errorf("got: %v, want: %v", gotHeader, nil)
	}
}

func TestWriteFiles(t *testing.T) {
	chunkFilename := "test_chunk"
	chunkPath := "data/test/" + chunkFilename
	header := []string{"id", "first_name", "last_name", "email", "gender", "ip_address"}
	chunks := [][][]string{
		{
			{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
			{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
		},
		{
			{"3", "Gerri", "Choffin", "gchoffin2@ning.com", "Male", "9.254.198.50"},
			{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
		},
	}

	gotErr := writeFiles(header, chunks, chunkPath)
	if gotErr != nil {
		t.Errorf("got: %v, want: %v", gotErr, nil)
	}

	f, _ := os.Open(chunkPath + "0.csv")
	reader := csv.NewReader(f)

	gotRows, _ := reader.ReadAll()
	wantRows := [][]string{
		{"id", "first_name", "last_name", "email", "gender", "ip_address"},
		{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
		{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
	}
	if !areSlicesOfSlicesEqual(gotRows, wantRows) {
		t.Errorf("got: %v, want: %v", gotRows, wantRows)
	}

	_ = f.Close()
	_ = os.Remove(chunkPath + "0.csv")

	f, _ = os.Open(chunkPath + "1.csv")
	reader = csv.NewReader(f)

	gotRows, _ = reader.ReadAll()
	wantRows = [][]string{
		{"id", "first_name", "last_name", "email", "gender", "ip_address"},
		{"3", "Gerri", "Choffin", "gchoffin2@ning.com", "Male", "9.254.198.50"},
		{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
	}
	if !areSlicesOfSlicesEqual(gotRows, wantRows) {
		t.Errorf("got: %v, want: %v", gotRows, wantRows)
	}

	_ = f.Close()
	_ = os.Remove(chunkPath + "1.csv")
}

func TestExtract(t *testing.T) {
	filenameInput := "inputreadfilesomeinvalidrows_test.csv"
	filenamePath := "data/test/" + filenameInput
	chunkFilename := "test_chunk"
	chunkPath := "data/test/" + chunkFilename
	chunkSize := 2

	gotErr := Extract(filenamePath, chunkPath, chunkSize)
	if gotErr != nil {
		t.Errorf("got: %v, want: %v", gotErr, nil)
	}

	f, _ := os.Open(chunkPath + "0.csv")
	reader := csv.NewReader(f)

	gotRows, _ := reader.ReadAll()
	wantRows := [][]string{
		{"id", "first_name", "last_name", "email", "gender", "ip_address"},
		{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
		{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
	}
	if !areSlicesOfSlicesEqual(gotRows, wantRows) {
		t.Errorf("got: %v, want: %v", gotRows, wantRows)
	}

	_ = f.Close()
	_ = os.Remove(chunkPath + "0.csv")

	f, _ = os.Open(chunkPath + "1.csv")
	reader = csv.NewReader(f)

	gotRows, _ = reader.ReadAll()
	wantRows = [][]string{
		{"id", "first_name", "last_name", "email", "gender", "ip_address"},
		{"5", "Benoite", "Calve", "bjaffray4@github.com", "Female", "148.75.193.241"},
	}
	if !areSlicesOfSlicesEqual(gotRows, wantRows) {
		t.Errorf("got: %v, want: %v", gotRows, wantRows)
	}

	_ = f.Close()
	_ = os.Remove(chunkPath + "1.csv")
}
