package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// the line between metadata/masterfile fields is getting blurry; just lump them together
type tsGenericPidInfo struct {
	ID               int    `json:"id,omitempty"`
	Pid              string `json:"pid,omitempty"`
	Type             string `json:"type,omitempty"`
	Title            string `json:"title,omitempty"`
	Filename         string `json:"filename,omitempty"`
	TextSource       string `json:"text_source,omitempty"`
	OcrHint          string `json:"ocr_hint,omitempty"`
	OcrCandidate     bool   `json:"ocr_candidate,omitempty"`
	OcrLanguageHint  string `json:"ocr_language_hint,omitempty"`
	HasOcr           bool   `json:"has_ocr,omitempty"`
	HasTranscription bool   `json:"has_transcription,omitempty"`
	CatalogKey       string `json:"catalog_key,omitempty"`
	CallNumber       string `json:"call_number,omitempty"`
	imageSource      string
	remoteName       string
}

// holds metadata pid/page info
type tsPidInfo struct {
	Pid       tsGenericPidInfo
	Pages     []tsGenericPidInfo
	isOcrable bool
}

func getTsURL(api string, pid string, params map[string]string) string {
	url := fmt.Sprintf("%s%s/%s", config.tsAPIHost.value, api, pid)

	var qp []string
	for k, v := range params {
		if v != "" {
			qp = append(qp, fmt.Sprintf("%s=%s", k, v))
		}
	}

	if len(qp) > 0 {
		url += fmt.Sprintf("?%s", strings.Join(qp, "&"))
	}

	return url
}

func (c *clientContext) tsGetPagesFromManifest() ([]tsGenericPidInfo, error) {
	url := getTsURL("/api/manifest", c.req.pid, map[string]string{"unit": c.req.unit})

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		c.err("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("failed to create new manifest request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		c.err("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("failed to receive manifest response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsPages []tsGenericPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsPages); jErr != nil {
		c.err("Unmarshal() failed: %s", jErr.Error())
		return nil, fmt.Errorf("failed to unmarshal manifest response: [%s]", buf)
	}

	c.info("pid %s has %d pages", c.req.pid, len(tsPages))
	/*
		for i, p := range tsPages {
			c.info("    [page %d / %d]  { [%s]  [%s]  [%s]  [%s] }", i+1, len(tsPages), p.Pid, p.Filename, p.Title, p.TextSource)
		}
	*/

	return tsPages, nil
}

func (c *clientContext) tsGetPidInfo() (*tsPidInfo, error) {
	url := getTsURL("/api/pid", c.req.pid, nil)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		c.err("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("failed to create new pid request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		c.err("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("failed to receive pid response")
	}

	defer res.Body.Close()

	// parse json from body

	var ts tsPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &ts.Pid); jErr != nil {
		c.err("Unmarshal() failed: %s", jErr.Error())
		return nil, fmt.Errorf("failed to unmarshal pid response: [%s]", buf)
	}

	c.info("Type            : [%s]", ts.Pid.Type)
	c.info("TextSource      : [%s]", ts.Pid.TextSource)
	c.info("OcrHint         : [%s]", ts.Pid.OcrHint)
	c.info("OcrCandidate    : [%t]", ts.Pid.OcrCandidate)
	c.info("OcrLanguageHint : [%s]", ts.Pid.OcrLanguageHint)

	switch {
	case ts.Pid.Type == "master_file":
		c.info("    [page 1 / 1]  { [%s]  [%s]  [%s]  [%s] }", ts.Pid.Pid, ts.Pid.Filename, ts.Pid.Title, ts.Pid.TextSource)

		ts.Pages = append(ts.Pages, ts.Pid)
		return &ts, nil

	case strings.Contains(ts.Pid.Type, "metadata"):
		var mfErr error

		ts.Pages, mfErr = c.tsGetPagesFromManifest()
		if mfErr != nil {
			c.err("tsGetPagesFromManifest() failed: [%s]", mfErr.Error())
			return nil, mfErr
		}

		return &ts, nil
	}

	return nil, fmt.Errorf("unhandled PID type: [%s]", ts.Pid.Type)
}

func (c *clientContext) tsGetMetadataPidInfo() (*tsPidInfo, error) {
	ts, err := c.tsGetPidInfo()

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

func (c *clientContext) tsGetText(pid string) (string, error) {
	url := fmt.Sprintf("%s/api/pid/%s/text", config.tsAPIHost.value, pid)
	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		c.err("NewRequest() failed: %s", reqErr.Error())
		return "", errors.New("failed to create new fulltext request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		c.err("client.Do() failed: %s", resErr.Error())
		return "", errors.New("failed to receive fulltext response")
	}

	defer res.Body.Close()

	// read text from body

	text, textErr := ioutil.ReadAll(res.Body)
	if textErr != nil {
		c.err("ReadAll() failed: %s", textErr.Error())
		return "", errors.New("failed to read fulltext response")
	}

	return cleanOcrText(string(text)), nil
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

func (c *clientContext) tsPostText(pid, text string) error {
	form := url.Values{
		"text": {text},
		"key":  {config.tsAPIKey.value},
	}
	encodedForm := form.Encode()

	url := fmt.Sprintf("%s/api/pid/%s/ocr", config.tsAPIHost.value, pid)
	req, reqErr := http.NewRequest("POST", url, strings.NewReader(encodedForm))
	if reqErr != nil {
		c.err("NewRequest() failed: %s", reqErr.Error())
		return errors.New("failed to create new fulltext post request")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(encodedForm)))

	res, resErr := client.Do(req)
	if resErr != nil {
		c.err("client.Do() failed: %s", resErr.Error())
		return errors.New("failed to receive fulltext post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	c.info("[%s] posted ocr: [%s] <= [%s] (%d)", pid, buf, textSnippet(text), len(text))

	return nil
}

func (c *clientContext) tsJobStatusCallback(apiURL, status, message, started, finished string) error {
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
		c.err("Failed to serialize callback json: [%s]", jsonErr.Error())
		return errors.New("failed to serialze job status callback json")
	}

	req, reqErr := http.NewRequest("POST", apiURL, bytes.NewBuffer(output))
	if reqErr != nil {
		c.err("NewRequest() %s failed: %s", apiURL, reqErr.Error())
		return errors.New("failed to create new job status post request")
	}

	req.Header.Add("Content-type", "application/json")
	res, resErr := client.Do(req)
	if resErr != nil {
		c.err("client.Do() %s failed: %s", apiURL, resErr.Error())
		return errors.New("failed to receive job status post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)
	c.info("posted job status: [%s]; to: [%s]; response: [%s]", string(output), apiURL, buf)

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
