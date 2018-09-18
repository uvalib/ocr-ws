package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"github.com/rs/cors"
)

const version = "0.1"

type pageInfo struct {
	PID      string
	Filename string
	Title    sql.NullString
	txtFile  string
}

var db *sql.DB
var logger *log.Logger

/**
 * Main entry point for the web service
 */
func main() {
	/*
		lf, _ := os.OpenFile("service.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		defer lf.Close()
		logger = log.New(lf, "service: ", log.LstdFlags)
	*/
	// use below to log to console....
	logger = log.New(os.Stdout, "", log.LstdFlags)

	// Load cfg
	logger.Printf("===> ocr-ws staring up <===")
	logger.Printf("Load configuration...")
	getConfigValues()

	// Init DB connection
	logger.Printf("Init DB connection...")
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?allowOldPasswords=%s", config.dbUser.value, config.dbPass.value,
		config.dbHost.value, config.dbName.value, strconv.FormatBool(config.dbAllowOldPasswords.value))

	var err error
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		fmt.Printf("Database connection failed: %s", err.Error())
		os.Exit(1)
	}
	defer db.Close()

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
