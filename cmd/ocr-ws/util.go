package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/satori/go.uuid"
)

type ocrPidInfo struct {
	pid   string // page pid
	title string
	text  string
}

type ocrResultsInfo struct {
	pid     string // request pid
	reqid   string
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

func getIIIFUrl(pid string) string {
	url := strings.Replace(config.iiifUrlTemplate.value, "{PID}", pid, -1)
	return url
}

func writeFileWithContents(filename, contents string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0664)

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

func processEmails(workdir, subject, body, attachment string) {
	if emails, err := reqGetEmails(workdir); err == nil {
		for _, e := range emails {
			emailResults(e, subject, body, attachment)
		}
	} else {
		logger.Printf("error retrieving email addresses: [%s]", err.Error())
	}
}

func processCallbacks(workdir, reqid, status, message string) {
	started, finished, timesErr := reqGetTimes(workdir, reqid)
	if timesErr != nil {
		logger.Printf("could not get times; making some up")
		started = "2019-02-07 01:23:45 AM"
		finished = "2019-02-07 12:34:56 PM"
	}

	if callbacks, err := reqGetCallbacks(workdir); err == nil {
		for _, c := range callbacks {
			tsJobStatusCallback(c, status, message, started, finished)
		}
	} else {
		logger.Printf("error retrieving callbacks: [%s]", err.Error())
	}
}

func processOcrSuccess(res ocrResultsInfo) {
	logger.Printf("[%s] processing and posting successful OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid, currentTimestamp())

	ocrAllText := ""
	ocrAllFile := fmt.Sprintf("%s/ocr.txt", res.workDir)

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

	processEmails(res.workDir, fmt.Sprintf("OCR Results for %s", res.pid), "OCR results are attached.", ocrAllFile)
	processCallbacks(res.workDir, res.reqid, "success", "OCR completed successfully")

	os.RemoveAll(res.workDir)
}

func processOcrFailure(res ocrResultsInfo) {
	logger.Printf("[%s] processing failed OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid, currentTimestamp())

	processEmails(res.workDir, fmt.Sprintf("OCR Failure for %s", res.pid), fmt.Sprintf("OCR failure details: %s", res.details), "")
	processCallbacks(res.workDir, res.reqid, "fail", res.details)

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

func countsToString(m map[string]int) string {
	b := new(bytes.Buffer)

	for key, value := range m {
		fmt.Fprintf(b, "%s x %d; ", key, value)
	}

	return b.String()
}

func currentTimestamp() string {
	return time.Now().Format("2006-01-02 03:04:05 PM")
}
