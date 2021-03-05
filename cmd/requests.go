package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type reqInfo struct {
	ReqID         string
	Started       string
	Finished      string
	AWSWorkflowID string
	AWSRunID      string
}

func reqFileName(path string) string {
	return fmt.Sprintf("%s/requests.db", path)
}

func reqOpenDatabase(path string) (*sql.DB, error) {
	dbFile := reqFileName(path)

	//log.Printf("[req] using db file: [%s]", dbFile)

	return sql.Open("sqlite3", dbFile)
}

func reqInProgressByDates(req *reqInfo) bool {
	log.Printf("checking for existing request in progress by timestamps")

	// check if finished is set
	if req.Finished != "" {
		log.Printf("request has a finished timestamp; not in progress")
		return false
	}

	// check if started is more than 1 hour ago (assume S3 uploads will never take that long)
	secs := int64(3600)

	started, err := epochToInt64(req.Started)
	if err != nil {
		log.Printf("failure parsing start time: [%s] (%s); treating as not in progress", req.Started, err.Error())
		return false
	}

	now := time.Now().Unix()

	elapsed := now - started

	log.Printf("started: [%d]  now: [%d] => elapsed: [%d] > secs: [%d] ?", started, now, elapsed, secs)

	if elapsed > secs {
		log.Printf("request has a stale started timestamp; not in progress")
		return false
	}

	// somewhat recent, and not in SWF yet -- might be new/still uploading images
	log.Printf("request is somewhat recent, but not found in SWF; assuming in progress (could still be uploading to S3)")

	return true
}

func reqInProgress(path string) bool {
	log.Printf("checking for existing request in progress")

	req, err := reqGetRequestInfo(path, "")
	if err != nil {
		log.Printf("error getting request info; not in progress (%s)", err.Error())
		return false
	}

	if req.AWSWorkflowID == "" || req.AWSRunID == "" {
		log.Printf("no valid request info found; checking timestamps")

		return reqInProgressByDates(req)
	}

	log.Printf("found existing workflowID: [%s] / runID: [%s]", req.AWSWorkflowID, req.AWSRunID)

	// check if this is an open workflow
	open, openErr := awsWorkflowIsOpen(req.AWSWorkflowID, req.AWSRunID)
	if openErr == nil && open == true {
		log.Printf("workflow execution is open; in progress")
		return true
	}

	// check if this is a closed workflow
	closed, closedErr := awsWorkflowIsClosed(req.AWSWorkflowID, req.AWSRunID)
	if closedErr == nil && closed == true {
		log.Printf("workflow execution is closed; not in progress")
		return false
	}

	log.Printf("workflow execution is indeterminate; checking timestamps")

	return reqInProgressByDates(req)
}

func reqInitialize(path, reqid string) error {
	os.RemoveAll(path)
	os.MkdirAll(path, 0775)

	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		log.Printf("[req] failed to open requests database when initializing: [%s]", err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	// attempt to create tables, even if they exist

	query := `create table if not exists request_info (id integer not null primary key, req_id text unique, started text, finished text, aws_workflow_id text, aws_run_id text);`
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("[req] failed to create request_info table: [%s]", err.Error())
		log.Printf("%q", err)
		return errors.New("failed to create request info table")
	}

	query = `create table if not exists recipients (id integer not null primary key, type integer, value text unique);`
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("[req] failed to create recipients table: [%s]", err.Error())
		log.Printf("%q", err)
		return errors.New("failed to create recipients table")
	}

	tx, txErr := db.Begin()
	if txErr != nil {
		log.Printf("[req] failed to create request transaction: [%s]", txErr.Error())
		return errors.New("failed to create request transaction")
	}
	stmt, err := tx.Prepare("insert into request_info (req_id, started, finished, aws_workflow_id, aws_run_id) values (?, '', '', '', '');")
	if err != nil {
		log.Printf("[req] failed to prepare request transaction: [%s]", err.Error())
		return errors.New("failed to prepare request transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(reqid)
	if err != nil {
		log.Printf("[req] failed to execute request transaction: [%s]", err.Error())
		return errors.New("failed to execute request transaction")
	}
	tx.Commit()

	return nil
}

func reqGetRequestInfo(path, reqid string) (*reqInfo, error) {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		log.Printf("[req] failed to open requests database when getting workflow id: [%s]", err.Error())
		return nil, errors.New("failed to open requests database")
	}
	defer db.Close()

	// grab request info

	var req reqInfo
	var clause string

	if reqid != "" {
		clause = fmt.Sprintf(" where req_id = '%s'", reqid)
	}

	query := fmt.Sprintf("select req_id, started, finished, aws_workflow_id, aws_run_id from request_info%s;", clause)
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[req] failed to retrieve request info: [%s]", err.Error())
		return nil, errors.New("failed to retrieve request info")
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&req.ReqID, &req.Started, &req.Finished, &req.AWSWorkflowID, &req.AWSRunID)
		if err != nil {
			log.Printf("[req] failed to scan request info: [%s]", err.Error())
			return nil, errors.New("failed to scan request info")
		}
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		log.Printf("[req] select query failed: [%s]", err.Error())
		return nil, errors.New("failed to select request info")
	}

	log.Printf("req: %+v", req)

	return &req, nil
}

func reqUpdateRequestColumn(path, reqid, column, value string) error {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		log.Printf("[req] failed to open requests database when updating %s: [%s]", column, err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	query := fmt.Sprintf("update request_info set %s = '%s' where req_id = '%s';", column, value, reqid)
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("[req] failed to update %s: [%s]", column, err.Error())
		return fmt.Errorf("failed to update %s", column)
	}

	return nil
}

func reqUpdateStarted(path, reqid string) error {
	return reqUpdateRequestColumn(path, reqid, "started", fmt.Sprintf("%d", time.Now().Unix()))
}

func reqUpdateFinished(path, reqid string) error {
	return reqUpdateRequestColumn(path, reqid, "finished", fmt.Sprintf("%d", time.Now().Unix()))
}

func reqUpdateAwsWorkflowID(path, reqid, value string) error {
	return reqUpdateRequestColumn(path, reqid, "aws_workflow_id", value)
}

func reqUpdateAwsRunID(path, reqid, value string) error {
	return reqUpdateRequestColumn(path, reqid, "aws_run_id", value)
}

func reqAddRecipientByType(path string, rtype int, rvalue string) error {
	if rvalue == "" {
		return nil
	}

	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		log.Printf("[req] failed to open requests database when adding email: [%s]", err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	// insert a new row
	tx, txErr := db.Begin()
	if txErr != nil {
		log.Printf("[req] failed to create recipient transaction: [%s]", txErr.Error())
		return errors.New("failed to create recipient transaction")
	}
	stmt, err := tx.Prepare("insert into recipients (type, value) values(?, ?)")
	if err != nil {
		log.Printf("[req] failed to prepare recipient transaction: [%s]", err.Error())
		return errors.New("failed to prepare recipient transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(rtype, rvalue)
	if err != nil {
		log.Printf("[req] failed to execute recipient transaction: [%s]", err.Error())
		return errors.New("failed to execute recipient transaction")
	}
	tx.Commit()

	return nil
}

func reqAddEmail(path, value string) error {
	return reqAddRecipientByType(path, 1, value)
}

func reqAddCallback(path, value string) error {
	return reqAddRecipientByType(path, 2, value)
}

func reqGetRecipientsByType(path string, rtype int) ([]string, error) {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		log.Printf("[req] failed to open requests database when getting recipients: [%s]", err.Error())
		return nil, errors.New("failed to open requests database")
	}
	defer db.Close()

	// grab unique values

	var values []string

	query := fmt.Sprintf("select value from recipients where type = %d;", rtype)
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("[req] failed to retrieve values: [%s]", err.Error())
		return nil, errors.New("failed to retrieve values")
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			log.Printf("[req] failed to scan value: [%s]", err.Error())
			return nil, errors.New("failed to scan value")
		}

		values = appendStringIfMissing(values, value)
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		log.Printf("[req] select query failed: [%s]", err.Error())
		return nil, errors.New("failed to select values")
	}

	return values, nil
}

func reqGetEmails(path string) ([]string, error) {
	return reqGetRecipientsByType(path, 1)
}

func reqGetCallbacks(path string) ([]string, error) {
	return reqGetRecipientsByType(path, 2)
}
