package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// fields common to multiple API response structs

type tsAPICommonFields struct {
	Id       int    `json:"id,omitempty"`
	Pid      string `json:"pid,omitempty"`
	Title    string `json:"title,omitempty"`
	Filename string `json:"filename,omitempty"`
}

// /api/pid/:PID response

type tsAPIPidClonedFrom struct {
	tsAPICommonFields
}

type tsAPIPidResponse struct {
	tsAPICommonFields
	Type               string             `json:"type,omitempty"`
	AvailabilityPolicy string             `json:"availability_policy,omitempty"`
	TextSource         string             `json:"text_source,omitempty"`
	OcrHint            string             `json:"ocr_hint,omitempty"`
	OcrLanguageHint    string             `json:"ocr_language_hint,omitempty"`
	ClonedFrom         tsAPIPidClonedFrom `json:"cloned_from,omitempty"`
}

// /api/manifest/:PID response

type tsAPIManifestFile struct {
	tsAPICommonFields
	Width  int `json:"width,omitempty"`
	Height int `json:"height,omitempty"`
}

// unused because json response is unnamed array
type tsAPIManifestResponse struct {
	Files []tsAPIManifestFile
}

// holds pid/page info
type tsPidInfo struct {
	tsAPIPidResponse
	Pages []tsAPICommonFields
}

func tsGetPagesFromManifest(ocr ocrInfo, w http.ResponseWriter) ([]tsAPICommonFields, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetManifestTemplate.value)
	url = strings.Replace(url, "{PID}", ocr.req.pid, 1)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new manifest request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to receive manifest response")
	}

	defer res.Body.Close()

	// parse json from body

	var tsManifestInfo []tsAPIManifestFile

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsManifestInfo); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal manifest response: [%s]", buf))
	}

	var tsPages []tsAPICommonFields

	for _, p := range tsManifestInfo {
		tsPages = append(tsPages, tsAPICommonFields{Pid: p.Pid, Filename: p.Filename, Title: p.Title})
	}

	return tsPages, nil
}

func tsGetPidInfo(ocr ocrInfo, w http.ResponseWriter) (*tsPidInfo, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetPidTemplate.value)
	url = strings.Replace(url, "{PID}", ocr.req.pid, 1)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return nil, errors.New("Failed to create new pid request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return nil, errors.New("Failed to receive pid response")
	}

	defer res.Body.Close()

	// parse json from body

	var ts tsPidInfo

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &ts); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal pid response: [%s]", buf))
	}

	logger.Printf("Type               : [%s]", ts.Type)
	logger.Printf("AvailabilityPolicy : [%s]", ts.AvailabilityPolicy)
	logger.Printf("TextSource         : [%s]", ts.TextSource)
	logger.Printf("OcrHint            : [%s]", ts.OcrHint)
	logger.Printf("OcrLanguageHint    : [%s]", ts.OcrLanguageHint)

	/*
	   mysql> select distinct(ocr_language_hint) from metadata where length(ocr_language_hint) = 3;
	   +-------------------+
	   | ocr_language_hint |
	   +-------------------+
	   | eng               |
	   | fra               |
	   | spa               |
	   | ara               |
	   | deu               |
	   | rus               |
	   +-------------------+
	   6 rows in set (0.00 sec)

	   mysql> select * from ocr_hints;
	   +----+-----------------+---------------+
	   | id | name            | ocr_candidate |
	   +----+-----------------+---------------+
	   |  1 | Modern Font     |             1 |
	   |  2 | Non-Text Image  |             0 |
	   |  3 | Handwritten     |             0 |
	   |  4 | Illegible       |             0 |
	   |  5 | Pre-Modern Font |             0 |
	   +----+-----------------+---------------+
	   5 rows in set (0.00 sec)
	*/

	switch {
	case strings.Contains(ts.Type, "metadata"):
		var mfErr error
		ts.Pages, mfErr = tsGetPagesFromManifest(ocr, w)
		if mfErr != nil {
			logger.Printf("tsGetPagesFromManifest() failed: [%s]", mfErr.Error())
			return nil, mfErr
		}
		return &ts, nil
	case ts.Type == "component":
		return nil, errors.New("PID is a component")
	}

	// sometimes pid is missing?  just use what we knew it to be:
	// (seems to be fixed as of 5.22.0, but dev is still on 5.20.1, so we leave this code in for now)
	//ts.Pages = append(ts.Pages, tsAPICommonFields{Pid: ts.Pid, Filename: ts.Filename, Title: ts.Title})
	ts.Pages = append(ts.Pages, tsAPICommonFields{Pid: ocr.req.pid, Filename: ts.Filename, Title: ts.Title})

	return &ts, nil
}

func tsGetText(pid string) (string, error) {
	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiGetFullTextTemplate.value)
	url = strings.Replace(url, "{PID}", pid, 1)

	req, reqErr := http.NewRequest("GET", url, nil)
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return "", errors.New("Failed to create new fulltext request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return "", errors.New("Failed to receive fulltext response")
	}

	defer res.Body.Close()

	// read text from body

	text, textErr := ioutil.ReadAll(res.Body)
	if textErr != nil {
		logger.Printf("ReadAll() failed: %s", textErr.Error())
		return "", errors.New("Failed to read fulltext response")
	}

	return string(text), nil
}

func tsPostText(pid, text string) error {
	form := url.Values{
		"text": {text},
	}

	url := fmt.Sprintf("%s%s", config.tsApiHost.value, config.tsApiPostFullTextTemplate.value)
	url = strings.Replace(url, "{PID}", pid, 1)

	req, reqErr := http.NewRequest("POST", url, strings.NewReader(form.Encode()))
	if reqErr != nil {
		logger.Printf("NewRequest() failed: %s", reqErr.Error())
		return errors.New("Failed to create new fulltext post request")
	}

	res, resErr := client.Do(req)
	if resErr != nil {
		logger.Printf("client.Do() failed: %s", resErr.Error())
		return errors.New("Failed to receive fulltext post response")
	}

	defer res.Body.Close()

	buf, _ := ioutil.ReadAll(res.Body)

	logger.Printf("Posted OCR; response: [%s]", buf)

	return nil
}
