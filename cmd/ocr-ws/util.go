package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

type ocrPidInfo struct {
	pid  string // page pid
	text string
}

type ocrResultsInfo struct {
	pid     string // request pid
	details string
	workDir string
	pages   []ocrPidInfo
}

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

func processOcrSuccess(res ocrResultsInfo) {
	ocrAllText := ""
	ocrAllFile := fmt.Sprintf("%s/ocr.txt", res.workDir)

	for _, p := range res.pages {
		// save to one file
		ocrOneFile := fmt.Sprintf("%s/%s.txt", res.workDir, p.pid)
		writeFileWithContents(ocrOneFile, p.text)

		ocrAllText = fmt.Sprintf("%s\n\n%s\n", ocrAllText, p.text)

		// post to tracksys
		//func tsPostText(pid, text string)
	}

	// save to all file
	if err := writeFileWithContents(ocrAllFile, ocrAllText); err != nil {
		logger.Printf("error creating results attachment file: [%s]", err.Error())
		return
	}

	emails, err := sqlGetEmails(res.workDir)

	if err != nil {
		logger.Printf("error retrieving email addresses: [%s]", err.Error())
		return
	}

	for _, e := range emails {
		emailResults(e, fmt.Sprintf("OCR Results for %s", res.pid), "OCR results are attached.", ocrAllFile)
	}

	//os.RemoveAll(res.workDir)
}

func processOcrFailure(res ocrResultsInfo) {
	emails, err := sqlGetEmails(res.workDir)

	if err != nil {
		logger.Printf("error retrieving email addresses: [%s]", err.Error())
		return
	}

	for _, e := range emails {
		emailResults(e, fmt.Sprintf("OCR Failure for %s", res.pid), fmt.Sprintf("OCR failure details: %s", res.details), "")
	}

	//os.RemoveAll(res.workDir)
}
