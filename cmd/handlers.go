package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type ocrRequest struct {
	pid      string
	unit     string
	email    string
	callback string
	force    string
	lang     string
}

type ocrInfo struct {
	req     ocrRequest // values from original request
	ts      *tsPidInfo // values looked up in tracksys
	subDir  string
	workDir string
	reqID   string
}

/**
 * Handle a request for OCR of page images
 */
func ocrGenerateHandler(c *gin.Context) {
	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = c.Param("pid")
	ocr.req.unit = c.Query("unit")
	ocr.req.email = c.Query("email")
	ocr.req.callback = c.Query("callback")
	ocr.req.force = c.Query("force")
	ocr.req.lang = c.Query("lang")

	// save info generated from the original request
	ocr.subDir = ocr.req.pid
	ocr.workDir = getWorkDir(ocr.subDir)
	ocr.reqID = randomID()

	// check if forcing ocr... bypasses all checks except pid existence (e.g. allows individual master_file ocr)
	if b, err := strconv.ParseBool(ocr.req.force); err == nil && b == true {
		ts, tsErr := tsGetPidInfo(ocr.req.pid, ocr.req.unit)

		if tsErr != nil {
			log.Printf("Tracksys API error: [%s]", tsErr.Error())
			c.String(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
			return
		}

		ocr.ts = ts

		c.String(http.StatusOK, "OK")

		go generateOcr(ocr)

		return
	}

	// normal request:

	// see if request is already in progress
	inProgress, _ := reqInProgress(ocr.workDir)
	if inProgress == true {
		// request is in progress; don't start another request, just add email/callback to completion notification list
		log.Printf("Request already in progress; adding email/callback to completion notification list")
		reqAddEmail(ocr.workDir, ocr.req.email)
		reqAddCallback(ocr.workDir, ocr.req.callback)
		c.String(http.StatusOK, "OK")
		return
	}

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		log.Printf("Tracksys API error: [%s]", tsErr.Error())
		c.String(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	ocr.ts = ts

	/*
		// shouldn't happen from virgo?

		// check if ocr/transcription already exists; if so, just email now

		if ocr.ts.Pid.TextSource != "" {
			log.Printf("OCR/transcription already exists; emailing now")

			reqAddEmail(ocr.workDir, ocr.req.email)
			reqAddCallback(ocr.workDir, ocr.req.callback)

			res := ocrResultsInfo{}

			res.pid = ocr.req.pid
			res.reqid = ocr.reqID
			res.workDir = ocr.workDir

			for _, p := range ocr.ts.Pages {
				txt, txtErr := tsGetText(p.Pid)
				if txtErr != nil {
					log.Printf("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
					res.details = "Error encountered while retrieving text for one or more pages"
					processOcrFailure(res)
					c.String(http.StatusInternalServerError, "ERROR: Could not retrieve page text")
					return
				}

				res.pages = append(res.pages, ocrPidInfo{pid: p.Pid, title: p.Title, text: txt})
			}

			processOcrSuccess(res)

			c.String(http.StatusOK, "OK")
			return
		}
	*/

	// check if this is ocr-able

	if ocr.ts.isOcrable == false {
		log.Printf("Cannot OCR: [%s]", ocr.ts.Pid.OcrHint)
		c.String(http.StatusBadRequest, "ERROR: PID is not in a format conducive to OCR")
		return
	}

	// perform ocr

	c.String(http.StatusOK, "OK")

	go generateOcr(ocr)
}

func getTextForMetadataPid(ts *tsPidInfo) (string, error) {
	var ocrText strings.Builder

	// preallocate buffer with assumed worst-case of 4K bytes per page
	ocrText.Grow(len(ts.Pages) * 4096)

	for i, p := range ts.Pages {
		pageText, txtErr := tsGetText(p.Pid)
		if txtErr != nil {
			log.Printf("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
			return "", errors.New("could not retrieve page text")
		}

		fmt.Fprintf(&ocrText, "[Title: %s]\n[Page: %d of %d]\n\n%s\n\n", p.Title, i+1, len(ts.Pages), pageText)
	}

	return ocrText.String(), nil
}

func ocrTextHandler(c *gin.Context) {
	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = c.Param("pid")
	ocr.req.unit = c.Query("unit")

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		log.Printf("Tracksys API error: [%s]", tsErr.Error())
		c.String(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	ocrText, txtErr := getTextForMetadataPid(ts)

	if txtErr != nil {
		c.String(http.StatusInternalServerError, fmt.Sprintf("ERROR: %s", txtErr.Error()))
		return
	}

	c.String(http.StatusOK, ocrText)
}

func ocrStatusHandler(c *gin.Context) {
	ocr := ocrInfo{}

	// save fields from original request
	ocr.req.pid = c.Param("pid")
	ocr.req.unit = c.Query("unit")

	// save info generated from the original request
	ocr.subDir = ocr.req.pid
	ocr.workDir = getWorkDir(ocr.subDir)

	ts, tsErr := tsGetMetadataPidInfo(ocr.req.pid, ocr.req.unit)

	if tsErr != nil {
		log.Printf("Tracksys API error: [%s]", tsErr.Error())
		c.String(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	status := make(map[string]interface{})

	status["has_ocr"] = ts.Pid.HasOcr
	status["has_transcription"] = ts.Pid.HasTranscription
	status["is_ocr_candidate"] = ts.isOcrable

	inProgress, pct := reqInProgress(ocr.workDir)
	if inProgress == true {
		status["ocr_progress"] = pct
	}

	c.JSON(http.StatusOK, status)
}

func generateOcr(ocr ocrInfo) {
	// check for language override
	if ocr.req.lang != "" {
		ocr.ts.Pid.OcrLanguageHint = ocr.req.lang
	}

	reqInitialize(ocr.workDir, ocr.reqID)
	reqUpdateStarted(ocr.workDir, ocr.reqID)
	reqUpdateImagesTotal(ocr.workDir, ocr.reqID, len(ocr.ts.Pages))
	reqAddEmail(ocr.workDir, ocr.req.email)
	reqAddCallback(ocr.workDir, ocr.req.callback)

	if err := awsGenerateOcr(ocr); err != nil {
		log.Printf("generateOcr() failed: [%s]", err.Error())

		res := ocrResultsInfo{}

		res.pid = ocr.req.pid
		res.reqid = ocr.reqID
		res.workDir = ocr.workDir
		res.details = "Error encountered while starting the OCR process"

		processOcrFailure(res)
	}
}
