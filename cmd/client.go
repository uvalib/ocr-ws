package main

import (
	"fmt"
	"log"
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
	ts      *tsPidInfo // values looked up in tracksys
	subDir  string
	workDir string
	reqID   string
}

type clientContext struct {
	ctx   *gin.Context
	reqID string     // unique request id for this connection
	ip    string     // client ip address
	req   ocrRequest // values from original request
	ocr   ocrInfo    // values derived while processing request
}

func newClientContext(ctx *gin.Context) *clientContext {
	c := clientContext{}
	c.initClient(ctx)
	return &c
}

func newBackgroundContext() *clientContext {
	c := clientContext{}
	c.initBackground()
	return &c
}

func (c *clientContext) initClient(ctx *gin.Context) {
	c.reqID = fmt.Sprintf("%08x", randomSource.Uint32())

	c.ctx = ctx

	c.ip = c.ctx.ClientIP()

	c.req.pid = c.ctx.Param("pid")
	c.req.unit = c.ctx.Query("unit")
	c.req.email = c.ctx.Query("email")
	c.req.callback = c.ctx.Query("callback")
	c.req.force = c.ctx.Query("force")
	c.req.lang = c.ctx.Query("lang")

	// save info generated from the original request
	c.ocr.subDir = c.req.pid
	c.ocr.workDir = getWorkDir(c.ocr.subDir)
	c.ocr.reqID = randomID()

	c.logRequest()
}

func (c *clientContext) initBackground() {
	c.reqID = "internal"
	c.ip = "internal"
}

func (c *clientContext) log(format string, args ...interface{}) {
	parts := []string{
		fmt.Sprintf("[ip:%s]", c.ip),
		fmt.Sprintf("[req:%s]", c.reqID),
		fmt.Sprintf(format, args...),
	}

	log.Printf("%s", strings.Join(parts, " "))
}

func (c *clientContext) debug(format string, args ...interface{}) {
	c.log("DEBUG: "+format, args...)
}

func (c *clientContext) info(format string, args ...interface{}) {
	c.log("INFO: "+format, args...)
}

func (c *clientContext) warn(format string, args ...interface{}) {
	c.log("WARNING: "+format, args...)
}

func (c *clientContext) err(format string, args ...interface{}) {
	c.log("ERROR: "+format, args...)
}

func (c *clientContext) logRequest() {
	query := ""
	if c.ctx.Request.URL.RawQuery != "" {
		query = fmt.Sprintf("?%s", c.ctx.Request.URL.RawQuery)
	}

	c.log("REQUEST: %s %s%s", c.ctx.Request.Method, c.ctx.Request.URL.Path, query)
}

func (c *clientContext) logResponse(code int, msg string) {
	c.log("RESPONSE: status: %d (%s)", code, msg)
}

func (c *clientContext) respondString(code int, msg string) {
	c.logResponse(code, msg)
	c.ctx.String(code, msg)
}

func (c *clientContext) respondJSON(code int, data interface{}) {
	c.logResponse(code, "json")
	c.ctx.JSON(code, data)
}
