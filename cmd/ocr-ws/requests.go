package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type reqInfo struct {
	ReqId string
	Started string
	Finished string
	AWSWorkflowId string
	AWSRunId string
}

func reqFileName(path string) string {
	return fmt.Sprintf("%s/requests.db", path)
}

func reqOpenDatabase(path string) (*sql.DB, error) {
	dbFile := reqFileName(path)

	//logger.Printf("[req] using db file: [%s]", dbFile)

	return sql.Open("sqlite3", dbFile)
}

func reqInProgressByDates(req *reqInfo) bool {
	logger.Printf("checking for existing request in progress by timestamps")

	// check if finished is set
	if req.Finished != "" {
		logger.Printf("request has a finished timestamp; not in progress")
		return false
	}

	// check if started is more than 1 hour ago (assume S3 uploads will never take that long)
	secs := int64(3600)

	started, err := epochToInt64(req.Started)
	if err != nil {
		logger.Printf("failure parsing start time: [%s] (%s); treating as not in progress", req.Started, err.Error())
		return false
	}

	now := time.Now().Unix()

	elapsed := now - started

	logger.Printf("started: [%d]  now: [%d] => elapsed: [%d] > secs: [%d] ?", started, now, elapsed, secs)

	if elapsed > secs {
		logger.Printf("request has a stale started timestamp; not in progress")
		return false
	}

	// somewhat recent, and not in SWF yet -- might be new/still uploading images
	logger.Printf("request is somewhat recent, but not found in SWF; assuming in progress (could still be uploading to S3)")

	return true
}

func reqInProgress(path string) bool {
	logger.Printf("checking for existing request in progress")

	req, err := reqGetRequestInfo(path, "")
	if err != nil {
		logger.Printf("error getting request info; not in progress (%s)", err.Error())
		return false
	}

	if req.AWSWorkflowId == "" || req.AWSRunId == "" {
		logger.Printf("no valid request info found; checking timestamps")

		return reqInProgressByDates(req)
	}

	logger.Printf("found existing workflowId: [%s] / runId: [%s]", req.AWSWorkflowId, req.AWSRunId)

	// check if this is an open workflow
	open, openErr := awsWorkflowIsOpen(req.AWSWorkflowId, req.AWSRunId)
	if openErr == nil && open == true {
		logger.Printf("workflow execution is open; in progress")
		return true
	}

	// check if this is a closed workflow
	closed, closedErr := awsWorkflowIsClosed(req.AWSWorkflowId, req.AWSRunId)
	if closedErr == nil && closed == true {
		logger.Printf("workflow execution is closed; not in progress")
		return false
	}

	logger.Printf("workflow execution is indeterminate; checking timestamps")

	return reqInProgressByDates(req)
}

func reqInitialize(path, reqid string) error {
	os.RemoveAll(path)
	os.MkdirAll(path, 0775)

	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		logger.Printf("[req] failed to open requests database when initializing: [%s]", err.Error())
		return errors.New("Failed to open requests database")
	}
	defer db.Close()

	// attempt to create tables, even if they exist

	query := `create table if not exists request_info (id integer not null primary key, req_id text unique, started text, finished text, aws_workflow_id text, aws_run_id text);`
	_, err = db.Exec(query)
	if err != nil {
		logger.Printf("[req] failed to create request_info table: [%s]", err.Error())
		logger.Printf("%q", err)
		return errors.New("Failed to create request info table")
	}

	query = `create table if not exists recipients (id integer not null primary key, type integer, value text unique);`
	_, err = db.Exec(query)
	if err != nil {
		logger.Printf("[req] failed to create recipients table: [%s]", err.Error())
		logger.Printf("%q", err)
		return errors.New("Failed to create recipients table")
	}

	tx, txErr := db.Begin()
	if txErr != nil {
		logger.Printf("[req] failed to create request transaction: [%s]", txErr.Error())
		return errors.New("Failed to create request transaction")
	}
	stmt, err := tx.Prepare("insert into request_info (req_id, started, finished, aws_workflow_id, aws_run_id) values (?, '', '', '', '');")
	if err != nil {
		logger.Printf("[req] failed to prepare request transaction: [%s]", err.Error())
		return errors.New("Failed to prepare request transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(reqid)
	if err != nil {
		logger.Printf("[req] failed to execute request transaction: [%s]", err.Error())
		return errors.New("Failed to execute request transaction")
	}
	tx.Commit()

	return nil
}

func reqGetRequestInfo(path, reqid string) (*reqInfo, error) {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		logger.Printf("[req] failed to open requests database when getting workflow id: [%s]", err.Error())
		return nil, errors.New("Failed to open requests database")
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
		logger.Printf("[req] failed to retrieve request info: [%s]", err.Error())
		return nil, errors.New("Failed to retrieve request info")
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&req.ReqId, &req.Started, &req.Finished, &req.AWSWorkflowId, &req.AWSRunId)
		if err != nil {
			logger.Printf("[req] failed to scan request info: [%s]", err.Error())
			return nil, errors.New("Failed to scan request info")
		}
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		logger.Printf("[req] select query failed: [%s]", err.Error())
		return nil, errors.New("Failed to select requear info")
	}

	logger.Printf("req: %+v", req)

	return &req, nil
}

func reqUpdateRequestColumn(path, reqid, column, value string) error {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		logger.Printf("[req] failed to open requests database when updating %s: [%s]", column, err.Error())
		return errors.New("Failed to open requests database")
	}
	defer db.Close()

	query := fmt.Sprintf("update request_info set %s = '%s' where req_id = '%s';", column, value, reqid)
	_, err = db.Exec(query)
	if err != nil {
		logger.Printf("[req] failed to update %s: [%s]", column, err.Error())
		return errors.New(fmt.Sprintf("Failed to update %s", column))
	}

	return nil
}

func reqUpdateStarted(path, reqid string) error {
	return reqUpdateRequestColumn(path, reqid, "started", fmt.Sprintf("%d", time.Now().Unix()))
}

func reqUpdateFinished(path, reqid string) error {
	return reqUpdateRequestColumn(path, reqid, "finished", fmt.Sprintf("%d", time.Now().Unix()))
}

func reqUpdateAwsWorkflowId(path, reqid, value string) error {
	return reqUpdateRequestColumn(path, reqid, "aws_workflow_id", value)
}

func reqUpdateAwsRunId(path, reqid, value string) error {
	return reqUpdateRequestColumn(path, reqid, "aws_run_id", value)
}

func reqAddRecipientByType(path string, rtype int, rvalue string) error {
	if rvalue == "" {
		return nil
	}

	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		logger.Printf("[req] failed to open requests database when adding email: [%s]", err.Error())
		return errors.New("Failed to open requests database")
	}
	defer db.Close()

	// insert a new row
	tx, txErr := db.Begin()
	if txErr != nil {
		logger.Printf("[req] failed to create recipient transaction: [%s]", txErr.Error())
		return errors.New("Failed to create recipient transaction")
	}
	stmt, err := tx.Prepare("insert into recipients (type, value) values(?, ?)")
	if err != nil {
		logger.Printf("[req] failed to prepare recipient transaction: [%s]", err.Error())
		return errors.New("Failed to prepare recipient transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(rtype, rvalue)
	if err != nil {
		logger.Printf("[req] failed to execute recipient transaction: [%s]", err.Error())
		return errors.New("Failed to execute recipient transaction")
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
		logger.Printf("[req] failed to open requests database when getting recipients: [%s]", err.Error())
		return nil, errors.New("Failed to open requests database")
	}
	defer db.Close()

	// grab unique values

	var values []string

	query := fmt.Sprintf("select value from recipients where type = %d;", rtype)
	rows, err := db.Query(query)
	if err != nil {
		logger.Printf("[req] failed to retrieve values: [%s]", err.Error())
		return nil, errors.New("Failed to retrieve values")
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			logger.Printf("[req] failed to scan value: [%s]", err.Error())
			return nil, errors.New("Failed to scan value")
		}

		values = appendStringIfMissing(values, value)
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		logger.Printf("[req] select query failed: [%s]", err.Error())
		return nil, errors.New("Failed to select values")
	}

	return values, nil
}

func reqGetEmails(path string) ([]string, error) {
	return reqGetRecipientsByType(path, 1)
}

func reqGetCallbacks(path string) ([]string, error) {
	return reqGetRecipientsByType(path, 2)
}
