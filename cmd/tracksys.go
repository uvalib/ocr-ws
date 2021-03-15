package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// the line between metadata/masterfile fields is getting blurry; just lump them together
type tsGenericPidInfo struct {
	ID              int    `json:"id,omitempty"`
	Pid             string `json:"pid,omitempty"`
	Type            string `json:"type,omitempty"`
	Title           string `json:"title,omitempty"`
	Filename        string `json:"filename,omitempty"`
	TextSource      string `json:"text_source,omitempty"`
	OcrHint         string `json:"ocr_hint,omitempty"`
	OcrCandidate    bool   `json:"ocr_candidate,omitempty"`
	OcrLanguageHint string `json:"ocr_language_hint,omitempty"`
	imageSource     string
	remoteName      string
}

// holds metadata pid/page info
type tsPidInfo struct {
	Pid       tsGenericPidInfo
	Pages     []tsGenericPidInfo
	isOcrable bool
}

func getTsURL(api string, pid string, unit string) string {
	url := fmt.Sprintf("%s%s/%s", config.tsAPIHost.value, api, pid)
	if unit != "" {
		url = fmt.Sprintf("%s?unit=%s", url, unit)
	}
	return url
}

func tsGetPagesFromManifest(pid, unit string) ([]tsGenericPidInfo, error) {
	url := getTsURL("/api/manifest", pid, unit)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		log.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("failed to create new manifest request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		log.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("failed to receive manifest response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsPages []tsGenericPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsPages); jErr != nil {
		log.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, fmt.Errorf("failed to unmarshal manifest response: [%s]", buf)
	}

	for i, p := range tsPages {
		log.Printf("    [page %d / %d]  { [%s]  [%s]  [%s]  [%s] }", i+1, len(tsPages), p.Pid, p.Filename, p.Title, p.TextSource)
	}

	return tsPages, nil
}

func tsGetPidInfo(pid, unit string) (*tsPidInfo, error) {
	url := getTsURL("/api/pid", pid, "")

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		log.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("failed to create new pid request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		log.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("failed to receive pid response")
	}

	defer res.Body.Close()

	// parse json from body

	var ts tsPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &ts.Pid); jErr != nil {
		log.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, fmt.Errorf("failed to unmarshal pid response: [%s]", buf)
	}

	log.Printf("Type            : [%s]", ts.Pid.Type)
	log.Printf("TextSource      : [%s]", ts.Pid.TextSource)
	log.Printf("OcrHint         : [%s]", ts.Pid.OcrHint)
	log.Printf("OcrCandidate    : [%t]", ts.Pid.OcrCandidate)
	log.Printf("OcrLanguageHint : [%s]", ts.Pid.OcrLanguageHint)

	switch {
	case ts.Pid.Type == "master_file":
		log.Printf("    [page 1 / 1]  { [%s]  [%s]  [%s]  [%s] }", ts.Pid.Pid, ts.Pid.Filename, ts.Pid.Title, ts.Pid.TextSource)

		ts.Pages = append(ts.Pages, ts.Pid)
		return &ts, nil

	case strings.Contains(ts.Pid.Type, "metadata"):
		var mfErr error

		ts.Pages, mfErr = tsGetPagesFromManifest(pid, unit)
		if mfErr != nil {
			log.Printf("tsGetPagesFromManifest() failed: [%s]", mfErr.Error())
			return nil, mfErr
		}

		return &ts, nil
	}

	return nil, fmt.Errorf("unhandled PID type: [%s]", ts.Pid.Type)
}

func tsGetMetadataPidInfo(pid, unit string) (*tsPidInfo, error) {
	ts, err := tsGetPidInfo(pid, unit)

	if err != nil {
		return nil, err
	}

	if strings.Contains(ts.Pid.Type, "metadata") == false {
		return nil, fmt.Errorf("pid is not a metadata type: [%s]", ts.Pid.Type)
	}

	// ensure there are pages to process
	if len(ts.Pages) == 0 {
		return nil, errors.New("metadata pid does not have any pages")
	}

	// check if this is ocr-able: FIXME (DCMD-634)
	ts.isOcrable = false
	if ts.Pid.OcrCandidate == true {
		if ts.Pid.TextSource == "" || ts.Pid.TextSource == "ocr" {
			ts.isOcrable = true
		}
	} else {
		// fallback for tracksysdev until it has the new API
		if ts.Pid.OcrHint == "Regular Font" || ts.Pid.OcrHint == "Modern Font" {
			ts.isOcrable = true
		}
	}

	return ts, nil
}

func tsGetText(pid string) (string, error) {
	url := getTsURL("/api/fulltext", pid, "") + "?type=transcription"

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		log.Printf("NewRequest() failed: %s", reqErr.Error())
		return "", errors.New("failed to create new fulltext request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		log.Printf("client.Do() failed: %s", resErr.Error())
		return "", errors.New("failed to receive fulltext response")
	}

	defer res.Body.Close()

	// read text from body

	text, textErr := ioutil.ReadAll(res.Body)
	if textErr != nil {
		log.Printf("ReadAll() failed: %s", textErr.Error())
		return "", errors.New("failed to read fulltext response")
	}

	return string(text), nil
}

func textSnippet(text string) string {
	txtLen := 48
	etcStr := "..."
	padLen := txtLen + len(etcStr)

	s := strings.Join(strings.Fields(text), " ")
	strLen := len(s)

	if strLen > txtLen {
		s = s[:txtLen] + etcStr
	}

	s = s + strings.Repeat(" ", padLen)

	return s[:padLen]
}

func tsPostText(pid, text string) error {
	// if url not set, just skip over this

	if config.tsReadOnly.value == true {
		log.Printf("SKIPPING TRACKSYS POST")
		return nil
	}

	form := url.Values{
		"text": {text},
	}

	url := getTsURL("/api/fulltext", pid, "") + "/ocr"

	req, reqErr := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	if reqErr != nil {
		log.Printf("NewRequest() failed: %s", reqErr.Error())
		return errors.New("failed to create new fulltext post request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		log.Printf("client.Do() failed: %s", resErr.Error())
		return errors.New("failed to receive fulltext post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	log.Printf("[%s] posted ocr: [%s] <= [%s] (%d)", pid, buf, textSnippet(text), len(text))

	return nil
}

func tsJobStatusCallback(api, status, message, started, finished string) error {
	jobstatus := struct {
		Status   string `json:"status,omitempty"`
		Message  string `json:"message,omitempty"`
		Started  string `json:"started,omitempty"`
		Finished string `json:"finished,omitempty"`
	}{
		Status:   status,
		Message:  message,
		Started:  started,
		Finished: finished,
	}

	output, jsonErr := json.Marshal(jobstatus)
	if jsonErr != nil {
		log.Printf("Failed to serialize callback json: [%s]", jsonErr.Error())
		return errors.New("failed to serialze job status callback json")
	}

	form := url.Values{
		"json": {string(output)},
	}

	url := getTsURL(api, "", "")

	req, reqErr := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	if reqErr != nil {
		log.Printf("NewRequest() failed: %s", reqErr.Error())
		return errors.New("failed to create new job status post request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		log.Printf("client.Do() failed: %s", resErr.Error())
		return errors.New("failed to receive job status post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	log.Printf("posted job status: [%s]; response: [%s]", string(output), buf)

	return nil
}

func tsTimestamp(epoch string) string {
	e, err := epochToInt64(epoch)
	if err != nil {
		e = time.Now().Unix()
	}

	ts := time.Unix(e, 0).Format("2006-01-02 03:04:05 PM")

	return ts
}
