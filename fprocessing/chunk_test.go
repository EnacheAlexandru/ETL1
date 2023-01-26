package fprocessing

import (
	"encoding/csv"
	"etl/msgerr"
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

func TestAreSlicesOfSlicesEqual(t *testing.T) {
	cases := []struct {
		description string
		a           [][]int
		b           [][]int
		want        bool
	}{
		{
			description: "1. slices are equal",
			a:           [][]int{{1, 2}, {3}, {4, 5, 6}},
			b:           [][]int{{1, 2}, {3}, {4, 5, 6}},
			want:        true,
		},
		{
			description: "2. slices are not equal",
			a:           [][]int{{1, 2}, {3}, {4, 5, 6}},
			b:           [][]int{{1, 2}, {3}, {4, -5, 6}},
			want:        false,
		},
		{
			description: "3. one slice can be included in the other",
			a:           [][]int{{1, 2}, {3}, {4, 5, 6}},
			b:           [][]int{{1, 2}, {3}, {4, 5, 6}, {7}},
			want:        false,
		},
	}

	for _, c := range cases {
		got := areSlicesOfSlicesEqual(c.a, c.b)
		if got != c.want {
			t.Errorf("test case: %v\n got: %v\n want: %v", c.description, got, c.want)
		}
	}
}

// checks if an element is present in given array
func TestContains(t *testing.T) {
	cases := []struct {
		description string
		arr         []int
		elem        int
		want        bool
	}{
		{
			description: "1. element is present",
			arr:         []int{1, 2, 3, 4, 5},
			elem:        4,
			want:        true,
		},
		{
			description: "2. element is not present",
			arr:         []int{1, 2, 3, 4, 5},
			elem:        6,
			want:        false,
		},
	}

	for _, c := range cases {
		got := contains(c.arr, c.elem)
		if got != c.want {
			t.Errorf("test case: %v\n got: %v\n want: %v", c.description, got, c.want)
		}
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

func TestReadFile(t *testing.T) {
	cases := []struct {
		description string
		filename    string
		chunkSize   int
		wantErr     error
		wantChunks  [][][]string
		wantHeader  []string
	}{
		{
			description: "1. number of valid rows do not exactly divide by chunkSize",
			filename:    "data/test/somevalidrows_test.csv",
			chunkSize:   3,
			wantErr:     nil,
			wantChunks: [][][]string{
				{
					{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
					{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
					{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
				},
				{
					{"5", "Benoite", "Calve", "bjaffray4@github.com", "Female", "148.75.193.241"},
				},
			},
			wantHeader: []string{
				"id", "first_name", "last_name", "email", "gender", "ip_address",
			},
		},
		{
			description: "2. number of valid rows exactly divides by chunkSize",
			filename:    "data/test/somevalidrows_test.csv",
			chunkSize:   2,
			wantErr:     nil,
			wantChunks: [][][]string{
				{
					{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
					{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
				},
				{
					{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
					{"5", "Benoite", "Calve", "bjaffray4@github.com", "Female", "148.75.193.241"},
				},
			},
			wantHeader: []string{
				"id", "first_name", "last_name", "email", "gender", "ip_address",
			},
		},
		{
			description: "3. error reading input file",
			filename:    "data/test/invalid_file",
			chunkSize:   2,
			wantErr:     msgerr.ErrorFileOpen,
			wantChunks:  nil,
			wantHeader:  nil,
		},
	}

	for _, c := range cases {
		gotHeader, gotChunks, gotErr := readFile(c.filename, c.chunkSize)

		if gotErr != c.wantErr {
			t.Errorf("test case: %v\n got: %v\n want: %v", c.description, gotErr, c.wantErr)
		}

		if !areSlicesOfSlicesOfSlicesEqual(gotChunks, c.wantChunks) {
			t.Errorf("test case: %v\n got: %v\n want: %v", c.description, gotChunks, c.wantChunks)
		}

		if !areSlicesEqual(gotHeader, c.wantHeader) {
			t.Errorf("test case: %v\n got: %v\n want: %v", c.description, gotHeader, c.wantHeader)
		}
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

	var wantErr error = nil
	gotErr := writeFiles(header, chunks, chunkPath)
	if gotErr != wantErr {
		t.Errorf("got: %v, want: %v", gotErr, wantErr)
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
	filenameInput := "somevalidrows_test.csv"
	filenamePath := "data/test/" + filenameInput
	chunkFilename := "test_chunk"
	chunkPath := "data/test/" + chunkFilename
	chunkSize := 3

	var wantErr error = nil
	gotErr := Extract(filenamePath, chunkPath, chunkSize)
	if gotErr != wantErr {
		t.Errorf("got: %v, want: %v", gotErr, wantErr)
	}

	f, _ := os.Open(chunkPath + "0.csv")
	reader := csv.NewReader(f)

	gotRows, _ := reader.ReadAll()
	wantRows := [][]string{
		{"id", "first_name", "last_name", "email", "gender", "ip_address"},
		{"1", "Mavra", "Malec", "mmalec0@usa.gov", "Female", "229.215.245.102"},
		{"2", "Fan", "Gilvear", "fgilvear1@people.com.cn", "Female", "125.219.253.132"},
		{"4", "Tremayne", "Loosemore", "tloosemore3@cnn.com", "Male", "167.249.115.222"},
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

func TestExtractChunkTooSmall(t *testing.T) {
	filenameInput := "somevalidrows_test.csv"
	filenamePath := "data/test/" + filenameInput
	chunkFilename := "test_chunk"
	chunkPath := "data/test/" + chunkFilename
	chunkSize := 0

	wantErr := msgerr.ErrorChunkTooSmall
	gotErr := Extract(filenamePath, chunkPath, chunkSize)
	if gotErr != wantErr {
		t.Errorf("got: %v, want: %v", gotErr, wantErr)
	}
}
