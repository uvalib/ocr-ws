package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

/**
 * Handle a request for OCR of page images
 */
func ocrGenerateHandler(ctx *gin.Context) {
	c := newClientContext(ctx)

	// check if forcing ocr... bypasses all checks except pid existence (e.g. allows individual master_file ocr)
	if b, err := strconv.ParseBool(c.req.force); err == nil && b == true {
		ts, tsErr := c.tsGetPidInfo()

		if tsErr != nil {
			c.err("Tracksys API error: [%s]", tsErr.Error())
			c.respondString(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
			return
		}

		c.ocr.ts = ts

		c.respondString(http.StatusOK, "OK")

		go c.generateOcr()

		return
	}

	// normal request:

	// see if request is already in progress
	inProgress, _ := c.reqInProgress(c.ocr.workDir)
	if inProgress == true {
		// request is in progress; don't start another request, just add email/callback to completion notification list
		c.info("Request already in progress; adding email/callback to completion notification list")
		c.reqAddEmail(c.ocr.workDir, c.req.email)
		c.reqAddCallback(c.ocr.workDir, c.req.callback)
		c.respondString(http.StatusOK, "OK")
		return
	}

	ts, tsErr := c.tsGetMetadataPidInfo()

	if tsErr != nil {
		c.err("Tracksys API error: [%s]", tsErr.Error())
		c.respondString(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	c.ocr.ts = ts

	// check if ocr/transcription already exists; if so, just email now

	if ts.Pid.HasOcr == true {
		c.info("OCR/transcription already exists; emailing now")

		c.reqInitialize(c.ocr.workDir, c.ocr.reqID)
		c.reqUpdateCatalogKey(c.ocr.workDir, c.ocr.reqID, c.ocr.ts.Pid.CatalogKey)
		c.reqUpdateCallNumber(c.ocr.workDir, c.ocr.reqID, c.ocr.ts.Pid.CallNumber)
		c.reqAddEmail(c.ocr.workDir, c.req.email)
		c.reqAddCallback(c.ocr.workDir, c.req.callback)

		res := ocrResultsInfo{}

		res.pid = c.req.pid
		res.reqid = c.ocr.reqID
		res.workDir = c.ocr.workDir
		res.overwrite = false

		for _, p := range c.ocr.ts.Pages {
			txt, txtErr := c.tsGetText(p.Pid)
			if txtErr != nil {
				c.err("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
				res.details = "Error encountered while retrieving text for one or more pages"
				c.processOcrFailure(res)
				c.respondString(http.StatusInternalServerError, "ERROR: Could not retrieve page text")
				return
			}

			res.pages = append(res.pages, ocrPidInfo{pid: p.Pid, text: txt})
		}

		c.processOcrSuccess(res)

		c.respondString(http.StatusOK, "OK")
		return
	}

	// check if this is ocr-able

	if c.ocr.ts.isOcrable == false {
		c.err("Cannot OCR: [%s]", c.ocr.ts.Pid.OcrHint)
		c.respondString(http.StatusBadRequest, "ERROR: PID is not in a format conducive to OCR")
		return
	}

	// perform ocr

	c.respondString(http.StatusOK, "OK")

	go c.generateOcr()
}

func (c *clientContext) getTextForMetadataPid() (string, error) {
	var pages []string

	for _, p := range c.ocr.ts.Pages {
		pageText, txtErr := c.tsGetText(p.Pid)
		if txtErr != nil {
			c.err("[%s] tsGetText() error: [%s]", p.Pid, txtErr.Error())
			return "", errors.New("could not retrieve page text")
		}

		pages = append(pages, pageText)
	}

	ocrText := ocrFormatDocument(pages)

	return ocrText, nil
}

func ocrTextHandler(ctx *gin.Context) {
	c := newClientContext(ctx)

	ts, tsErr := c.tsGetMetadataPidInfo()

	if tsErr != nil {
		c.err("Tracksys API error: [%s]", tsErr.Error())
		c.respondString(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	c.ocr.ts = ts

	ocrText, txtErr := c.getTextForMetadataPid()

	if txtErr != nil {
		c.respondString(http.StatusInternalServerError, fmt.Sprintf("ERROR: %s", txtErr.Error()))
		return
	}

	c.respondString(http.StatusOK, ocrText)
}

func ocrStatusHandler(ctx *gin.Context) {
	c := newClientContext(ctx)

	ts, tsErr := c.tsGetMetadataPidInfo()

	if tsErr != nil {
		c.err("Tracksys API error: [%s]", tsErr.Error())
		c.respondString(http.StatusNotFound, fmt.Sprintf("ERROR: Could not retrieve PID info: [%s]", tsErr.Error()))
		return
	}

	status := make(map[string]interface{})

	status["has_ocr"] = ts.Pid.HasOcr
	status["has_transcription"] = ts.Pid.HasTranscription
	status["is_ocr_candidate"] = ts.isOcrable

	if inProgress, pct := c.reqInProgress(c.ocr.workDir); inProgress == true {
		c.info("request in progress: %s", pct)
		status["ocr_progress"] = pct
	} else {
		c.info("no request in progress")
	}

	c.respondJSON(http.StatusOK, status)
}

func (c *clientContext) generateOcr() {
	// check for language override
	if c.req.lang != "" {
		c.ocr.ts.Pid.OcrLanguageHint = c.req.lang
	}

	c.reqInitialize(c.ocr.workDir, c.ocr.reqID)
	c.reqUpdateStarted(c.ocr.workDir, c.ocr.reqID)
	c.reqUpdateImagesTotal(c.ocr.workDir, c.ocr.reqID, len(c.ocr.ts.Pages))
	c.reqUpdateCatalogKey(c.ocr.workDir, c.ocr.reqID, c.ocr.ts.Pid.CatalogKey)
	c.reqUpdateCallNumber(c.ocr.workDir, c.ocr.reqID, c.ocr.ts.Pid.CallNumber)
	c.reqAddEmail(c.ocr.workDir, c.req.email)
	c.reqAddCallback(c.ocr.workDir, c.req.callback)

	if err := c.awsGenerateOcr(); err != nil {
		c.err("generateOcr() failed: [%s]", err.Error())

		res := ocrResultsInfo{}

		res.pid = c.req.pid
		res.reqid = c.ocr.reqID
		res.workDir = c.ocr.workDir
		res.details = "Error encountered while starting the OCR process"

		c.processOcrFailure(res)
	}
}
