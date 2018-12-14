package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
)

const version = "0.1"

var db *sql.DB
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

	// Init DB connection
	logger.Printf("Init DB connection...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?allowOldPasswords=%s", config.tsDBUser.value, config.tsDBPass.value,
		config.tsDBHost.value, config.tsDBName.value, strconv.FormatBool(config.tsDBAllowOldPasswords.value))

	var err error
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("Database connection failed: %s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	// initialize AWS session
	sess = session.Must(session.NewSession())
	go awsPollForDecisionTasks()
	//	go awsSubmitTestWorkflows()

	// Set routes and start server
	mux := httprouter.New()
	mux.GET("/", rootHandler)
	mux.GET("/ocr/:pid", generateHandler)
	mux.GET("/ocr/:pid/status", statusHandler)
	mux.GET("/ocr/:pid/download", downloadHandler)
	mux.GET("/ocr/:pid/delete", deleteHandler)
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
