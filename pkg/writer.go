package chunks

import (
	"encoding/csv"
	"fmt"
	"os"
)

type Writer struct {
	BasePath  string
	Filename  string
	Extension string
	MaxBytes  int64
	filecount int

	csvWriter *csv.Writer
	csvFile   *os.File
	csvBytes  int64
}

func NewWriter() (*Writer, error) {
	w := &Writer{
		BasePath:  "/tmp/writer",
		Filename:  "collection-201",
		Extension: ".csv",
		MaxBytes:  1 * 1024 * 1024 * 1000, // 1 BYTE * 1024 (KB) * 1024 (MB) * 1000 (GB),
		filecount: 0,
	}
	err := w.NextFile()
	return w, err
}

func NewCSVWriter(filename string) (*os.File, *csv.Writer, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return nil, nil, err
	}

	writer := csv.NewWriter(f)
	return f, writer, nil
}

func (w *Writer) NextFile() error {
	newCount := w.filecount + 1
	filename := fmt.Sprintf("%s/%s_%d.%s", w.BasePath, w.Filename, newCount, w.Extension)
	cf, cw, err := NewCSVWriter(filename)
	if err != nil {
		return err
	}

	w.filecount = newCount
	w.csvFile = cf
	w.csvWriter = cw
	w.csvBytes = 0

	return nil
}

func (w *Writer) Write(line []string) error {
	lineBytes := 12
	for _, l := range line {
		lineBytes += len(l)
	}

	if w.csvBytes+int64(lineBytes) > w.MaxBytes {
		err := w.csvFile.Close()
		if err != nil {
			return err
		}

		err = w.NextFile()
		if err != nil {
			return err
		}
	}

	w.csvWriter.Write(line)
	w.csvWriter.Flush()
	w.csvBytes += int64(lineBytes)

	return w.csvWriter.Error()
}
