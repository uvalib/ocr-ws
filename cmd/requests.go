package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type reqInfo struct {
	ReqID          string
	Started        string
	Finished       string
	AWSWorkflowID  string
	AWSRunID       string
	ImagesUploaded string
	ImagesComplete string
	ImagesTotal    string
	CatalogKey     string
	CallNumber     string
}

func (c *clientContext) reqFileName(path string) string {
	return fmt.Sprintf("%s/requests.db", path)
}

func (c *clientContext) reqOpenDatabase(path string) (*sql.DB, error) {
	dbFile := c.reqFileName(path)

	//c.info("[SQL] using db file: [%s]", dbFile)

	return sql.Open("sqlite3", dbFile)
}

func (c *clientContext) reqInProgressByDates(req *reqInfo) bool {
	c.info("[SQL] checking for existing request in progress by timestamps")

	// check if finished is set
	if req.Finished != "" {
		c.info("[SQL] request has a finished timestamp; not in progress")
		return false
	}

	// check if started is more than 1 hour ago (assume S3 uploads will never take that long)
	secs := int64(3600)

	started, err := epochToInt64(req.Started)
	if err != nil {
		c.warn("[SQL] failure parsing start time: [%s] (%s); treating as not in progress", req.Started, err.Error())
		return false
	}

	now := time.Now().Unix()

	elapsed := now - started

	c.info("[SQL] started: [%d]  now: [%d] => elapsed: [%d] > secs: [%d] ?", started, now, elapsed, secs)

	if elapsed > secs {
		c.info("[SQL] request has a stale started timestamp; not in progress")
		return false
	}

	// somewhat recent, and not in SWF yet -- might be new/still uploading images
	c.info("[SQL] request is somewhat recent, but not found in SWF; assuming in progress (could still be uploading to S3)")

	return true
}

func (c *clientContext) reqInProgress(path string) (bool, string) {
	c.info("[SQL] checking for existing request in progress")

	zeroPct := "0%"

	req, err := c.reqGetRequestInfo(path, "")
	if err != nil {
		c.warn("[SQL] error getting request info; not in progress (%s)", err.Error())
		return false, zeroPct
	}

	uploaded, _ := strconv.Atoi(req.ImagesUploaded)
	complete, _ := strconv.Atoi(req.ImagesComplete)
	total, _ := strconv.Atoi(req.ImagesTotal)

	pct := zeroPct
	if total > 0 {
		stepsDone := uploaded + complete
		stepsTotal := 2 * total
		pct = fmt.Sprintf("%d%%", (100*stepsDone)/stepsTotal)
		c.debug("[SQL] progress: (%s + %s) / (2 * %s) => %d / %d => %s complete", req.ImagesUploaded, req.ImagesComplete, req.ImagesTotal, stepsDone, stepsTotal, pct)
	}

	if req.AWSWorkflowID == "" || req.AWSRunID == "" {
		c.info("[SQL] no valid request info found; checking timestamps")

		if c.reqInProgressByDates(req) == true {
			return true, pct
		}

		return false, zeroPct
	}

	c.info("[SQL] found existing workflowID: [%s] / runID: [%s]", req.AWSWorkflowID, req.AWSRunID)

	// check if this is an open workflow
	open, openErr := awsWorkflowIsOpen(req.AWSWorkflowID, req.AWSRunID)
	if openErr == nil && open == true {
		c.info("[SQL] workflow execution is open; in progress")
		return true, pct
	}

	// check if this is a closed workflow
	closed, closedErr := awsWorkflowIsClosed(req.AWSWorkflowID, req.AWSRunID)
	if closedErr == nil && closed == true {
		c.info("[SQL] workflow execution is closed; not in progress")
		return false, zeroPct
	}

	c.info("[SQL] workflow execution is indeterminate; checking timestamps")

	return c.reqInProgressByDates(req), pct
}

func (c *clientContext) reqInitialize(path, reqid string) error {
	os.RemoveAll(path)
	os.MkdirAll(path, 0775)

	// open database
	db, err := c.reqOpenDatabase(path)
	if err != nil {
		c.err("[SQL] failed to open requests database when initializing: [%s]", err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	// attempt to create tables, even if they exist

	query := `create table if not exists request_info (id integer not null primary key, req_id text unique, started text, finished text, aws_workflow_id text, aws_run_id text, images_uploaded text, images_complete text, images_total text, catalog_key text, call_number text);`
	_, err = db.Exec(query)
	if err != nil {
		c.err("[SQL] failed to create request_info table: [%s]", err.Error())
		c.err("[SQL] %q", err)
		return errors.New("failed to create request info table")
	}

	query = `create table if not exists recipients (id integer not null primary key, type integer, value text unique);`
	_, err = db.Exec(query)
	if err != nil {
		c.err("[SQL] failed to create recipients table: [%s]", err.Error())
		c.err("[SQL] %q", err)
		return errors.New("failed to create recipients table")
	}

	tx, txErr := db.Begin()
	if txErr != nil {
		c.err("[SQL] failed to create request transaction: [%s]", txErr.Error())
		return errors.New("failed to create request transaction")
	}
	stmt, err := tx.Prepare("insert into request_info (req_id, started, finished, aws_workflow_id, aws_run_id, images_uploaded, images_complete, images_total, catalog_key, call_number) values (?, '', '', '', '', '0', '0', '0', '', '');")
	if err != nil {
		c.err("[SQL] failed to prepare request transaction: [%s]", err.Error())
		return errors.New("failed to prepare request transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(reqid)
	if err != nil {
		c.err("[SQL] failed to execute request transaction: [%s]", err.Error())
		return errors.New("failed to execute request transaction")
	}
	tx.Commit()

	return nil
}

func (c *clientContext) reqGetRequestInfo(path, reqid string) (*reqInfo, error) {
	// open database
	db, err := c.reqOpenDatabase(path)
	if err != nil {
		c.err("[SQL] failed to open requests database when getting workflow id: [%s]", err.Error())
		return nil, errors.New("failed to open requests database")
	}
	defer db.Close()

	// grab request info

	var req reqInfo
	var clause string

	if reqid != "" {
		clause = fmt.Sprintf(" where req_id = '%s'", reqid)
	}

	query := fmt.Sprintf("select req_id, started, finished, aws_workflow_id, aws_run_id, images_uploaded, images_complete, images_total, catalog_key, call_number from request_info%s;", clause)
	rows, err := db.Query(query)
	if err != nil {
		// just warn here; existence of db isn't checked until this point so errors are excessively noisy
		c.warn("[SQL] failed to retrieve request info: [%s]", err.Error())
		return nil, errors.New("failed to retrieve request info")
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&req.ReqID, &req.Started, &req.Finished, &req.AWSWorkflowID, &req.AWSRunID, &req.ImagesUploaded, &req.ImagesComplete, &req.ImagesTotal, &req.CatalogKey, &req.CallNumber)
		if err != nil {
			c.err("[SQL] failed to scan request info: [%s]", err.Error())
			return nil, errors.New("failed to scan request info")
		}
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		c.err("[SQL] select query failed: [%s]", err.Error())
		return nil, errors.New("failed to select request info")
	}

	return &req, nil
}

func (c *clientContext) reqUpdateRequestColumn(path, reqid, column, value string) error {
	// open database
	db, err := c.reqOpenDatabase(path)
	if err != nil {
		c.err("[SQL] failed to open requests database when updating %s: [%s]", column, err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	query := fmt.Sprintf("update request_info set %s = '%s' where req_id = '%s';", column, value, reqid)
	_, err = db.Exec(query)
	if err != nil {
		c.err("[SQL] failed to update %s: [%s]", column, err.Error())
		return fmt.Errorf("failed to update %s", column)
	}

	return nil
}

func (c *clientContext) reqUpdateStarted(path, reqid string) error {
	return c.reqUpdateRequestColumn(path, reqid, "started", fmt.Sprintf("%d", time.Now().Unix()))
}

func (c *clientContext) reqUpdateFinished(path, reqid string) error {
	return c.reqUpdateRequestColumn(path, reqid, "finished", fmt.Sprintf("%d", time.Now().Unix()))
}

func (c *clientContext) reqUpdateAwsWorkflowID(path, reqid, value string) error {
	return c.reqUpdateRequestColumn(path, reqid, "aws_workflow_id", value)
}

func (c *clientContext) reqUpdateAwsRunID(path, reqid, value string) error {
	return c.reqUpdateRequestColumn(path, reqid, "aws_run_id", value)
}

func (c *clientContext) reqUpdateImagesUploaded(path, reqid string, value int) error {
	return c.reqUpdateRequestColumn(path, reqid, "images_uploaded", fmt.Sprintf("%d", value))
}

func (c *clientContext) reqUpdateImagesComplete(path, reqid string, value int) error {
	return c.reqUpdateRequestColumn(path, reqid, "images_complete", fmt.Sprintf("%d", value))
}

func (c *clientContext) reqUpdateImagesTotal(path, reqid string, value int) error {
	return c.reqUpdateRequestColumn(path, reqid, "images_total", fmt.Sprintf("%d", value))
}

func (c *clientContext) reqUpdateCatalogKey(path, reqid, value string) error {
	return c.reqUpdateRequestColumn(path, reqid, "catalog_key", value)
}

func (c *clientContext) reqUpdateCallNumber(path, reqid, value string) error {
	return c.reqUpdateRequestColumn(path, reqid, "call_number", value)
}

func (c *clientContext) reqAddRecipientByType(path string, rtype int, rvalue string) error {
	if rvalue == "" {
		return nil
	}

	// open database
	db, err := c.reqOpenDatabase(path)
	if err != nil {
		c.err("[SQL] failed to open requests database when adding email: [%s]", err.Error())
		return errors.New("failed to open requests database")
	}
	defer db.Close()

	// insert a new row
	tx, txErr := db.Begin()
	if txErr != nil {
		c.err("[SQL] failed to create recipient transaction: [%s]", txErr.Error())
		return errors.New("failed to create recipient transaction")
	}
	stmt, err := tx.Prepare("insert into recipients (type, value) values(?, ?)")
	if err != nil {
		c.err("[SQL] failed to prepare recipient transaction: [%s]", err.Error())
		return errors.New("failed to prepare recipient transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(rtype, rvalue)
	if err != nil {
		c.err("[SQL] failed to execute recipient transaction: [%s]", err.Error())
		return errors.New("failed to execute recipient transaction")
	}
	tx.Commit()

	return nil
}

func (c *clientContext) reqAddEmail(path, value string) error {
	return c.reqAddRecipientByType(path, 1, value)
}

func (c *clientContext) reqAddCallback(path, value string) error {
	return c.reqAddRecipientByType(path, 2, value)
}

func (c *clientContext) reqGetRecipientsByType(path string, rtype int) ([]string, error) {
	// open database
	db, err := c.reqOpenDatabase(path)
	if err != nil {
		c.err("[SQL] failed to open requests database when getting recipients: [%s]", err.Error())
		return nil, errors.New("failed to open requests database")
	}
	defer db.Close()

	// grab unique values

	var values []string

	query := fmt.Sprintf("select value from recipients where type = %d;", rtype)
	rows, err := db.Query(query)
	if err != nil {
		c.err("[SQL] failed to retrieve values: [%s]", err.Error())
		return nil, errors.New("failed to retrieve values")
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			c.err("[SQL] failed to scan value: [%s]", err.Error())
			return nil, errors.New("failed to scan value")
		}

		values = appendStringIfMissing(values, value)
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		c.err("[SQL] select query failed: [%s]", err.Error())
		return nil, errors.New("failed to select values")
	}

	return values, nil
}

func (c *clientContext) reqGetEmails(path string) ([]string, error) {
	return c.reqGetRecipientsByType(path, 1)
}

func (c *clientContext) reqGetCallbacks(path string) ([]string, error) {
	return c.reqGetRecipientsByType(path, 2)
}
