package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
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
	mux := httprouter.New()
	mux.GET("/", rootHandler)
	mux.GET("/version", versionHandler)
	mux.GET("/healthcheck", healthCheckHandler)

	mux.GET("/ocr/:pid", ocrGenerateHandler)
	mux.GET("/ocr/:pid/status", ocrStatusHandler)
	mux.GET("/ocr/:pid/text", ocrTextHandler)
	log.Printf("Start service on port %s", config.listenPort.value)

	log.Fatal(http.ListenAndServe(":"+config.listenPort.value, cors.Default().Handler(mux)))
}

// Handle a request for /
func rootHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Printf("%s %s", r.Method, r.RequestURI)
	fmt.Fprintf(w, "OCR service version %s", version)
}

// Handle a request for /version
func versionHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	output, jsonErr := json.Marshal(versionDetails)
	if jsonErr != nil {
		log.Printf("Failed to serialize output: [%s]", jsonErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Print(w, string(output))
}

// Handle a request for /healthcheck
func healthCheckHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	health := healthcheckDetails{healthCheckStatus{Healthy: true, Message: "Not implemented"}}

	output, jsonErr := json.Marshal(health)
	if jsonErr != nil {
		log.Printf("Failed to serialize output: [%s]", jsonErr.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fmt.Print(w, string(output))
}

//
// end of file
//
