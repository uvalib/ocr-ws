package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
)

const version = "0.1"

var logger *log.Logger
var sess *session.Session
var client *http.Client

/**
 * Main entry point for the web service
 */
func main() {
	logger = log.New(os.Stdout, "", log.LstdFlags)

	// Load cfg
	logger.Printf("===> ocr-ws starting up <===")
	logger.Printf("Load configuration...")
	getConfigValues()

	// initialize http client
	client = &http.Client{Timeout: 10 * time.Second}

	// initialize AWS session
	sess = session.Must(session.NewSession())
	go awsPollForDecisionTasks()

	// Set routes and start server
	mux := httprouter.New()
	mux.GET("/", rootHandler)
	mux.GET("/ocr/:pid", ocrGenerateHandler)
	mux.GET("/ocr/:pid/status", ocrStatusHandler)
	mux.GET("/ocr/:pid/text", ocrTextHandler)
	mux.GET("/healthcheck", healthCheckHandler)
	logger.Printf("Start service on port %s", config.listenPort.value)

	if config.useHttps.value == true {
		log.Fatal(http.ListenAndServeTLS(":"+config.listenPort.value, config.sslCrt.value, config.sslKey.value, cors.Default().Handler(mux)))
	} else {
		log.Fatal(http.ListenAndServe(":"+config.listenPort.value, cors.Default().Handler(mux)))
	}
}

/**
 * Handle a request for /
 */
func rootHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	logger.Printf("%s %s", r.Method, r.RequestURI)
	fmt.Fprintf(w, "OCR service version %s", version)
}
