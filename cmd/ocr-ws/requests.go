package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func reqFileName(path string) string {
	return fmt.Sprintf("%s/requests.db", path)
}

func reqDatabaseExists(path string) bool {
	dbFile := reqFileName(path)

	if _, err := os.Stat(dbFile); err == nil {
		return true
	}

	return false
}

func reqOpenDatabase(path string) (*sql.DB, error) {
	dbFile := reqFileName(path)

	//logger.Printf("[req] using db file: [%s]", dbFile)

	return sql.Open("sqlite3", dbFile)
}

func reqInitialize(path, reqid string) error {
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

func reqGetTimes(path, reqid string) (string, string, error) {
	// open database
	db, err := reqOpenDatabase(path)
	if err != nil {
		logger.Printf("[req] failed to open requests database when getting times: [%s]", err.Error())
		return "", "", errors.New("Failed to open requests database")
	}
	defer db.Close()

	// grab times

	var started, finished string

	query := fmt.Sprintf("select started, finished from request_info where req_id = '%s';", reqid)
	rows, err := db.Query(query)
	if err != nil {
		logger.Printf("[req] failed to retrieve times: [%s]", err.Error())
		return "", "", errors.New("Failed to retrieve times")
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&started, &finished)
		if err != nil {
			logger.Printf("[req] failed to scan times: [%s]", err.Error())
			return "", "", errors.New("Failed to scan times")
		}
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		logger.Printf("[req] select query failed: [%s]", err.Error())
		return "", "", errors.New("Failed to select times")
	}

	return started, finished, nil
}

func reqUpdateRequest(path, reqid, column, value string) error {
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

func reqUpdateStarted(path, reqid, value string) error {
	return reqUpdateRequest(path, reqid, "started", value)
}

func reqUpdateFinished(path, reqid, value string) error {
	return reqUpdateRequest(path, reqid, "finished", value)
}

func reqUpdateAwsWorkflowId(path, reqid, value string) error {
	return reqUpdateRequest(path, reqid, "aws_workflow_id", value)
}

func reqUpdateAwsRunId(path, reqid, value string) error {
	return reqUpdateRequest(path, reqid, "aws_run_id", value)
}

func reqAddRecipient(path string, rtype int, rvalue string) error {
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
	return reqAddRecipient(path, 1, value)
}

func reqAddCallback(path, value string) error {
	return reqAddRecipient(path, 2, value)
}

func reqGetRecipients(path string, rtype int) ([]string, error) {
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
	return reqGetRecipients(path, 1)
}

func reqGetCallbacks(path string) ([]string, error) {
	return reqGetRecipients(path, 2)
}
