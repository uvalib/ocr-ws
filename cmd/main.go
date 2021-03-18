package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

const version = "0.9.0"

var sess *session.Session
var client *http.Client

/**
 * Main entry point for the web service
 */
func main() {
	// Load cfg
	log.Printf("===> ocr-ws starting up <===")
	log.Printf("Load configuration...")
	getConfigValues()

	// load version details
	initVersion()

	// initialize http client
	client = &http.Client{Timeout: 10 * time.Second}

	// initialize AWS session
	if config.awsDisabled.value == false {
		sess = session.Must(session.NewSession())
		go awsPollForDecisionTasks()
	}

	// Set routes and start server
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()

	router := gin.Default()

	corsCfg := cors.DefaultConfig()
	corsCfg.AllowAllOrigins = true
	corsCfg.AllowCredentials = true
	corsCfg.AddAllowHeaders("Authorization")
	router.Use(cors.New(corsCfg))

	router.GET("/", rootHandler)
	router.GET("/robots.txt", robotsHandler)
	router.GET("/favicon.ico", ignoreHandler)
	router.GET("/version", versionHandler)
	router.GET("/healthcheck", healthCheckHandler)

	router.GET("/ocr/:pid", ocrGenerateHandler)
	router.GET("/ocr/:pid/status", ocrStatusHandler)
	router.GET("/ocr/:pid/text", ocrTextHandler)

	portStr := fmt.Sprintf(":%s", config.listenPort.value)
	log.Printf("Start service on %s", portStr)

	log.Fatal(router.Run(portStr))
}

// Handle a request for /
func rootHandler(c *gin.Context) {
	c.String(http.StatusOK, fmt.Sprintf("OCR service version %s", version))
}

// Handle a request for /robots.txt
func robotsHandler(c *gin.Context) {
	c.String(http.StatusOK, "User-agent: * Disallow: /")
}

// Handle a request for /favicon.ico
func ignoreHandler(c *gin.Context) {
}

// Handle a request for /version
func versionHandler(c *gin.Context) {
	output, jsonErr := json.Marshal(versionDetails)
	if jsonErr != nil {
		log.Printf("Failed to serialize output: [%s]", jsonErr.Error())
		c.String(http.StatusInternalServerError, "")
		return
	}

	c.String(http.StatusOK, string(output))
}

// Handle a request for /healthcheck
func healthCheckHandler(c *gin.Context) {
	health := healthcheckDetails{healthCheckStatus{Healthy: true, Message: "Not implemented"}}

	output, jsonErr := json.Marshal(health)
	if jsonErr != nil {
		log.Printf("Failed to serialize output: [%s]", jsonErr.Error())
		c.String(http.StatusInternalServerError, "")
		return
	}

	c.String(http.StatusOK, string(output))
}

//
// end of file
//
