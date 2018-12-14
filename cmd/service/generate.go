package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type pageInfo struct {
	PID      string
	Filename string
	Title    string
	txtFile  string
	lang     string
}

type ocrRequest struct {
	pid   string
	unit  string
	pages string
	token string
	email string
}

type ocrInfo struct {
	req     ocrRequest
	unitID  int
	workDir string
	destDir string
	pages   []pageInfo
}

func pagesEqual(a, b []pageInfo) bool {
	if len(a) != len(b) {
		return false
	}

	for i, _ := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

/**
 * Handle a request for OCR of page images
 */
func generateHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)

	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = params.ByName("pid")
	ocr.req.unit = r.URL.Query().Get("unit")
	ocr.req.email = r.URL.Query().Get("email")
	ocr.req.pages = r.URL.Query().Get("pages")
	ocr.req.token = r.URL.Query().Get("token")

	// save info generated from the original request
	ocr.unitID, _ = strconv.Atoi(ocr.req.unit)
	ocr.workDir = ocr.req.pid

	if ocr.unitID > 0 {
		// if pages from a specific unit are requested, put them
		// in a unit subdirectory under the metadata pid
		ocr.workDir = fmt.Sprintf("%s/%d", ocr.req.pid, ocr.unitID)
	}

	if len(ocr.req.pages) > 0 {
		if len(ocr.req.token) == 0 {
			logger.Printf("Request for partial OCR is missing a token")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Missing token")
			return
		}
		ocr.workDir = ocr.req.token
		logger.Printf("Request for partial OCR including pages: %s", ocr.req.pages)
	}

	// See if destination already extsts...
	ocr.destDir = fmt.Sprintf("%s/%s", config.storageDir.value, ocr.workDir)

	if _, err := os.Stat(ocr.destDir); err == nil {
		// path already exists; don't start another request, just start
		// normal completion polling routine
		logger.Printf("Request already in progress or completed")
		monitorProgressAndNotifyResults(ocr.workDir, ocr.req.pid, ocr.req.email)
		return
	}

	dbPages, dbErr := tsDBGetPages(ocr, w)
	apiPages, apiErr := tsAPIGetPages(ocr, w)

	if pagesEqual(dbPages, apiPages) == false {
		logger.Printf("pages not equal:")

		s := ""
		for _, p := range dbPages {
			s = s + p.PID + " "
		}
		logger.Printf("db pages: [%s]", s)

		s = ""
		for _, p := range apiPages {
			s = s + p.PID + " "
		}
		logger.Printf("apiPages: [%s]", s)
	}

	if dbErr != nil {
		logger.Printf("dbErr : %s", dbErr.Error())
	}

	if apiErr != nil {
		logger.Printf("apiErr: %s", apiErr.Error())
	}

	ocr.pages = apiPages

	// kick off lengthy OCR generation in a go routine
	go generateOcr(ocr)
}

func generateOcr(ocr ocrInfo) {
	logger.Printf("would ocr now...")
	//	localGenerateOcr(ocr)
	//	awsGenerateOcr(ocr)
}
