package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

func getWorkDir(subDir string) string {
	return fmt.Sprintf("%s/%s", config.storageDir.value, subDir)
}

func writeFileWithContents(filename, contents string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)

	if err != nil {
		logger.Printf("Unable to open file: %s", err.Error())
		return errors.New(fmt.Sprintf("Unable to open ocr file: [%s]", filename))
	}

	defer f.Close()

	w := bufio.NewWriter(f)

	if _, err = fmt.Fprintf(w, "%s", contents); err != nil {
		logger.Printf("Unable to write file: %s", err.Error())
		return errors.New(fmt.Sprintf("Unable to write ocr file: [%s]", filename))
	}

	w.Flush()

	return nil
}

func appendStringIfMissing(slice []string, str string) []string {
	for _, s := range slice {
		if s == str {
			return slice
		}
	}

	return append(slice, str)
}
