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
	OcrHintId          string             `json:"ocr_hint_id,omitempty"`
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

func tsGetPagesFromManifest(ocr ocrInfo, w http.ResponseWriter) (pages []pageInfo, err error) {
	url := strings.Replace(config.tsGetManifestUrlTemplate.value, "{PID}", ocr.req.pid, 1)

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

	for _, p := range tsManifestInfo {
		pages = append(pages, pageInfo{PID: p.Pid, Filename: p.Filename, Title: p.Title})
	}

	return pages, nil
}

func tsGetPages(ocr ocrInfo, w http.ResponseWriter) (pages []pageInfo, err error) {
	url := strings.Replace(config.tsGetPidUrlTemplate.value, "{PID}", ocr.req.pid, 1)

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

	var tsPidInfo tsAPIPidResponse

	buf, _ := ioutil.ReadAll(res.Body)
	if jErr := json.Unmarshal(buf, &tsPidInfo); jErr != nil {
		logger.Printf("Unmarshal() failed: %s", jErr.Error())
		return nil, errors.New(fmt.Sprintf("Failed to unmarshal pid response: [%s]", buf))
	}

	logger.Printf("Type               : [%s]", tsPidInfo.Type)
	logger.Printf("AvailabilityPolicy : [%s]", tsPidInfo.AvailabilityPolicy)
	logger.Printf("TextSource         : [%s]", tsPidInfo.TextSource)
	logger.Printf("OcrHintId          : [%s]", tsPidInfo.OcrHintId)
	logger.Printf("OcrLanguageHint    : [%s]", tsPidInfo.OcrLanguageHint)

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
	case strings.Contains(tsPidInfo.Type, "metadata"):
		return tsGetPagesFromManifest(ocr, w)
	case tsPidInfo.Type == "component":
		return nil, errors.New("PID is a component")
	}

	// sometimes pid is missing?  just use what we knew it to be:
	// (seems to be fixed as of 5.22.0, but dev is still on 5.20.1, so we leave this code in for now)
	//pages = append(pages, pageInfo{PID: tsPidInfo.Pid, Filename: tsPidInfo.Filename, Title: tsPidInfo.Title})
	pages = append(pages, pageInfo{PID: ocr.req.pid, Filename: tsPidInfo.Filename, Title: tsPidInfo.Title})

	return pages, nil
}

func tsGetText(pid string) (string, error) {
	url := strings.Replace(config.tsGetFullTextUrlTemplate.value, "{PID}", pid, 1)

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
	form := url.Values {
		"text": { text },
	}

	url := strings.Replace(config.tsPutFullTextUrlTemplate.value, "{PID}", pid, 1)

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
