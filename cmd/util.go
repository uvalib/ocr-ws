package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// structures

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

type serviceVersion struct {
	Version   string `json:"version,omitempty"`
	Build     string `json:"build,omitempty"`
	GoVersion string `json:"go_version,omitempty"`
}

type healthcheckDetails struct {
	Domain healthCheckStatus `json:"ocr_service,omitempty"`
}

type healthCheckStatus struct {
	Healthy bool   `json:"healthy,omitempty"`
	Message string `json:"message,omitempty"`
}

// globals

var randpool *rand.Rand
var versionDetails *serviceVersion

// functions

func initVersion() {
	buildVersion := "unknown"
	files, _ := filepath.Glob("buildtag.*")
	if len(files) == 1 {
		buildVersion = strings.Replace(files[0], "buildtag.", "", 1)
	}

	versionDetails = &serviceVersion{
		Version:   version,
		Build:     buildVersion,
		GoVersion: fmt.Sprintf("%s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH),
	}
}

func getWorkDir(subDir string) string {
	return fmt.Sprintf("%s/%s", config.storageDir.value, subDir)
}

func stripExtension(fileName string) string {
	strippedFileName := strings.TrimSuffix(fileName, filepath.Ext(fileName))

	return strippedFileName
}

func getLocalFilename(imgFile string) string {
	// "000012345_0123.tif" => ("000012345", "0123.tif")
	parts := strings.Split(imgFile, "_")
	localFile := fmt.Sprintf("%s/%s/%s", config.archiveDir.value, parts[0], imgFile)
	return localFile
}

func getRemoteFilename(imgFile, extensionSource string) string {
	// generates a remote filename based on the expected master tif location, but with the extension of the actual file used
	localFile := getLocalFilename(imgFile)
	remoteFile := fmt.Sprintf("%s%s", stripExtension(path.Base(localFile)), filepath.Ext(path.Base(extensionSource)))
	return remoteFile
}

func getS3Filename(reqID, remoteFile string) string {
	s3File := path.Join("requests", reqID, remoteFile)
	return s3File
}

func getIIIFUrl(pid string) string {
	url := strings.Replace(config.iiifURLTemplate.value, "{PID}", pid, -1)
	return url
}

func writeFileWithContents(filename, contents string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0664)

	if err != nil {
		log.Printf("Unable to open file: %s", err.Error())
		return fmt.Errorf("unable to open ocr file: [%s]", filename)
	}

	defer f.Close()

	w := bufio.NewWriter(f)

	if _, err = fmt.Fprintf(w, "%s", contents); err != nil {
		log.Printf("Unable to write file: %s", err.Error())
		return fmt.Errorf("unable to write ocr file: [%s]", filename)
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
		log.Printf("error retrieving email addresses: [%s]", err.Error())
	}
}

func processCallbacks(workdir, reqid, status, message string) {
	req, reqErr := reqGetRequestInfo(workdir, reqid)
	if reqErr != nil {
		log.Printf("could not get times; making some up.  error: [%s]", reqErr.Error())

		now := time.Now().Unix()
		then := now - 60

		req = &reqInfo{}
		req.Started = tsTimestamp(fmt.Sprintf("%d", then))
		req.Finished = tsTimestamp(fmt.Sprintf("%d", now))
	}

	if callbacks, err := reqGetCallbacks(workdir); err == nil {
		for _, c := range callbacks {
			tsJobStatusCallback(c, status, message, req.Started, req.Finished)
		}
	} else {
		log.Printf("error retrieving callbacks: [%s]", err.Error())
	}
}

func processOcrSuccess(res ocrResultsInfo) {
	log.Printf("[%s] processing and posting successful OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid)

	ocrAllText := ""
	ocrAllFile := fmt.Sprintf("%s/ocr.txt", res.workDir)

	for i, p := range res.pages {
		// save to one file
		ocrOneFile := fmt.Sprintf("%s/%s.txt", res.workDir, p.pid)

		headerPages := fmt.Sprintf("Page %d of %d", i+1, len(res.pages))
		headerBorder := strings.Repeat("=", len(headerPages))
		headerText := fmt.Sprintf("%s\n%s\n%s\n", headerBorder, headerPages, headerBorder)

		ocrOneText := fmt.Sprintf("%s\n%s\n", headerText, cleanOcrText(p.text))

		// save to page file
		if err := writeFileWithContents(ocrOneFile, ocrOneText); err != nil {
			log.Printf("[%s] error creating results page file: [%s]", res.pid, err.Error())
		}

		ocrAllText = fmt.Sprintf("%s\n%s\n", ocrAllText, ocrOneText)

		// post to tracksys

		if err := tsPostText(p.pid, p.text); err != nil {
			log.Printf("[%s] Tracksys OCR posting failed: [%s]", res.pid, err.Error())
		}
	}

	// save to all file
	if err := writeFileWithContents(ocrAllFile, ocrAllText); err != nil {
		log.Printf("[%s] error creating results attachment file: [%s]", res.pid, err.Error())
		res.details = "OCR generation process finalization failed"
		processOcrFailure(res)
		return
	}

	processEmails(res.workDir, fmt.Sprintf("OCR Results for %s", res.pid), "OCR results are attached.", ocrAllFile)
	processCallbacks(res.workDir, res.reqid, "success", "OCR completed successfully")

	os.RemoveAll(res.workDir)
}

func processOcrFailure(res ocrResultsInfo) {
	log.Printf("[%s] processing failed OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid)

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

func countsToString(m map[string]int) string {
	b := new(bytes.Buffer)

	for key, value := range m {
		fmt.Fprintf(b, "%s x %d; ", key, value)
	}

	return b.String()
}

func randomID() string {
	return fmt.Sprintf("%0x", randpool.Uint64())
}

func epochToInt64(epoch string) (int64, error) {
	e, err := strconv.ParseInt(epoch, 10, 64)
	if err != nil {
		return 0, err
	}

	return int64(e), nil
}

func cleanOcrText(text string) string {
	// matches two or more consecutive newlines
	squeezeLines := regexp.MustCompile(`\n\n+`)

	// matches strings with at least three alphanumeric characters anywhere.
	// this is fairly conservative but is actually pretty effective at removing truly noisy lines.
	validLine := regexp.MustCompile(`(?i)([[:alnum:]].*){3,}`)

	lines := strings.Split(text, "\n")

	var keep []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || validLine.MatchString(line) == true {
			keep = append(keep, line)
		}
	}

	s := strings.Join(keep, "\n")
	s = squeezeLines.ReplaceAllString(s, "\n\n")

	return s
}

func init() {
	randpool = rand.New(rand.NewSource(time.Now().UnixNano()))
}
