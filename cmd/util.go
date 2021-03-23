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
	pid  string // page pid
	text string
}

type ocrResultsInfo struct {
	pid       string // request pid
	reqid     string
	details   string
	workDir   string
	pages     []ocrPidInfo
	overwrite bool
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

func getVirgoURL(res ocrResultsInfo) string {
	v4url := "https://search.lib.virginia.edu"
	req, reqErr := reqGetRequestInfo(res.workDir, res.reqid)
	if reqErr != nil || req.CatalogKey == "" {
		v4url += fmt.Sprintf("/?q=keyword:{%s}", res.pid)
	} else {
		v4url += fmt.Sprintf("/items/%s", req.CatalogKey)
	}
	return v4url
}

func ocrFormatPageText(text string, i int, total int) string {
	headerPages := fmt.Sprintf("Page %d of %d", i, total)
	headerBorder := strings.Repeat("=", len(headerPages))
	headerText := fmt.Sprintf("%s\n%s\n%s\n", headerBorder, headerPages, headerBorder)

	pageText := fmt.Sprintf("%s\n%s\n", headerText, cleanOcrText(text))

	return pageText
}

func ocrFormatDocument(pages []string) string {
	doc := ""

	for i, page := range pages {
		pageText := ocrFormatPageText(page, i+1, len(pages))
		doc += "\n" + pageText + "\n"
	}

	return doc
}

func processOcrSuccess(res ocrResultsInfo) {
	log.Printf("[%s] processing and posting successful OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid)

	var pages []string
	for _, p := range res.pages {
		pages = append(pages, p.text)

		// post to tracksys?
		if res.overwrite == true {
			if err := tsPostText(p.pid, p.text); err != nil {
				log.Printf("[%s] Tracksys OCR posting failed: [%s]", res.pid, err.Error())
			}
		}
	}

	ocrBaseName := res.pid
	req, reqErr := reqGetRequestInfo(res.workDir, res.reqid)
	if reqErr == nil && req.CallNumber != "" {
		ocrBaseName = req.CallNumber
	}

	ocrText := ocrFormatDocument(pages)
	ocrFile := fmt.Sprintf("%s/%s.txt", res.workDir, ocrBaseName)

	// save to all file
	if err := writeFileWithContents(ocrFile, ocrText); err != nil {
		log.Printf("[%s] error creating results attachment file: [%s]", res.pid, err.Error())
		res.details = "OCR generation process finalization failed"
		processOcrFailure(res)
		return
	}

	subject := "Your OCR request is ready to view"
	body := fmt.Sprintf(`Hello,

The OCR document you requested is attached.

The file is also now discoverable in Virgo, along with citation and rights information: %s

Please note that it is your responsibility to determine appropriate rights and usage for Library material.

If you have questions about the OCR service, contact virgo-feedback@virginia.edu.

You can learn more about accessible Library services here: https://www.library.virginia.edu/services/accessibility-services/

Sincerely,

University of Virginia Library`, getVirgoURL(res))

	processEmails(res.workDir, subject, body, ocrFile)
	processCallbacks(res.workDir, res.reqid, "success", "OCR completed successfully")

	os.RemoveAll(res.workDir)
}

func processOcrFailure(res ocrResultsInfo) {
	log.Printf("[%s] processing failed OCR", res.pid)

	reqUpdateFinished(res.workDir, res.reqid)

	subject := "Your OCR request cannot be completed"
	body := fmt.Sprintf(`Hello,

Unfortunately, the OCR you requested for the item below has failed. This may be a result of a technical issue or a problem with the original document.

%s

If you have questions about the OCR service, contact virgo-feedback@virginia.edu.

You can learn more about accessible Library services here: https://www.library.virginia.edu/services/accessibility-services/

Sincerely,

University of Virginia Library`, getVirgoURL(res))

	processEmails(res.workDir, subject, body, "")
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
