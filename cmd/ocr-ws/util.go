package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/satori/go.uuid"
)

type ocrPidInfo struct {
	pid   string // page pid
	title string
	text  string
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

func getLocalFilename(imgFile string) string {
	// "000012345_0123.tif" => ("000012345", "0123.tif")
	parts := strings.Split(imgFile, "_")
	localFile := fmt.Sprintf("%s/%s/%s", config.archiveDir.value, parts[0], imgFile)
	return localFile
}

func getS3Filename(reqID, imgFile string) string {
	localFile := getLocalFilename(imgFile)
	baseFile := path.Base(localFile)
	parentDir := path.Base(path.Dir(localFile))
	s3File := path.Join("requests", reqID, parentDir, baseFile)
	return s3File
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

	logger.Printf("[%s] processing and posting successful OCR", res.pid)

	for i, p := range res.pages {
		// save to one file
		ocrOneFile := fmt.Sprintf("%s/%s.txt", res.workDir, p.pid)

		headerTitle := fmt.Sprintf("Title: %s", p.title)
		headerPages := fmt.Sprintf("Page %d of %d", i+1, len(res.pages))
		headerLength := maxOf(len(headerTitle), len(headerPages))
		headerBorder := strings.Repeat("=", headerLength)
		headerText := fmt.Sprintf("%s\n%s\n%s\n%s\n", headerBorder, headerTitle, headerPages, headerBorder)

		ocrOneText := fmt.Sprintf("%s\n%s\n", headerText, p.text)

		// save to page file
		if err := writeFileWithContents(ocrOneFile, ocrOneText); err != nil {
			logger.Printf("[%s] error creating results page file: [%s]", res.pid, err.Error())
		}

		ocrAllText = fmt.Sprintf("%s\n%s\n", ocrAllText, ocrOneText)

		// post to tracksys

		if err := tsPostText(p.pid, p.text); err != nil {
			logger.Printf("[%s] Tracksys OCR posting failed: [%s]", res.pid, err.Error())
		}
	}

	// save to all file
	if err := writeFileWithContents(ocrAllFile, ocrAllText); err != nil {
		logger.Printf("[%s] error creating results attachment file: [%s]", res.pid, err.Error())
		res.details = "OCR generation process finalization failed"
		processOcrFailure(res)
		return
	}

	emails, err := sqlGetEmails(res.workDir)

	if err != nil {
		logger.Printf("[%s] error retrieving email addresses: [%s]", res.pid, err.Error())
		return
	}

	for _, e := range emails {
		emailResults(e, fmt.Sprintf("OCR Results for %s", res.pid), "OCR results are attached.", ocrAllFile)
	}

	os.RemoveAll(res.workDir)
}

func processOcrFailure(res ocrResultsInfo) {
	logger.Printf("[%s] processing failed OCR", res.pid)

	emails, err := sqlGetEmails(res.workDir)

	if err != nil {
		logger.Printf("[%s] error retrieving email addresses: [%s]", res.pid, err.Error())
		return
	}

	for _, e := range emails {
		emailResults(e, fmt.Sprintf("OCR Failure for %s", res.pid), fmt.Sprintf("OCR failure details: %s", res.details), "")
	}

	os.RemoveAll(res.workDir)
}

func maxOf(ints ...int) int {
	max := ints[0]

	for _, n := range ints {
		if n > max {
			max = n
		}
	}

	return max
}

func newUUID() string {
	u := uuid.Must(uuid.NewV4())

	return u.String()
}
