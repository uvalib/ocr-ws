package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func sqlFileName(path string) string {
	return fmt.Sprintf("%s/requests.db", path)
}

func sqlDatabaseExists(path string) bool {
	dbFile := sqlFileName(path)

	if _, err := os.Stat(dbFile); err == nil {
		return true
	}

	return false
}

func sqlOpenDatabase(path string) (*sql.DB, error) {
	dbFile := sqlFileName(path)

	//logger.Printf("[sql] using db file: [%s]", dbFile)

	return sql.Open("sqlite3", dbFile)
}

func sqlRemoveDatabase(path string) {
	dbFile := sqlFileName(path)

	os.Remove(dbFile)
}

func sqlAddRecipients(path, email, callback string) error {
	os.MkdirAll(path, 0775)

	// open database
	db, err := sqlOpenDatabase(path)
	if err != nil {
		logger.Printf("[sql] failed to open requests database when adding email: [%s]", err.Error())
		return errors.New("Failed to open requests database")
	}
	defer db.Close()

	// attempt to create table, even if it exists
	sqlStmt := `create table if not exists ocr_requests (email text, callback text);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		logger.Printf("[sql] failed to create requests table: [%s]", err.Error())
		logger.Printf("%q", err)
		return errors.New("Failed to create requests table")
	}

	// insert a new row
	tx, err := db.Begin()
	if err != nil {
		logger.Printf("[sql] failed to create transaction: [%s]", err.Error())
		return errors.New("Failed to create request transaction")
	}
	stmt, err := tx.Prepare("insert into ocr_requests(email, callback) values(?, ?)")
	if err != nil {
		logger.Printf("[sql] failed to prepare transaction: [%s]", err.Error())
		return errors.New("Failed to prepare request transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(email, callback)
	if err != nil {
		logger.Printf("[sql] failed to execute transaction: [%s]", err.Error())
		return errors.New("Failed to execute request transaction")
	}
	tx.Commit()

	return nil
}

func sqlGetRecipients(path, rtype string) ([]string, error) {
	// open database
	db, err := sqlOpenDatabase(path)
	if err != nil {
		logger.Printf("[sql] failed to open requests database when getting %ss: [%s]", rtype, err.Error())
		return nil, errors.New("Failed to open requests database")
	}
	defer db.Close()

	// grab unique values

	var values []string

	query := fmt.Sprintf("select %s from ocr_requests where %s != '';", rtype, rtype)
	rows, err := db.Query(query)
	if err != nil {
		logger.Printf("[sql] failed to retrieve values: [%s]", err.Error())
		return nil, errors.New(fmt.Sprintf("Failed to retrieve %ss", rtype))
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			logger.Printf("[sql] failed to scan value: [%s]", err.Error())
			return nil, errors.New(fmt.Sprintf("Failed to scan %s", rtype))
		}

		values = appendStringIfMissing(values, value)
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		logger.Printf("[sql] select query failed: [%s]", err.Error())
		return nil, errors.New(fmt.Sprintf("Failed to select %ss", rtype))
	}

	return values, nil
}

func sqlGetEmails(path string) ([]string, error) {
	return sqlGetRecipients(path, "email")
}

func sqlGetCallbacks(path string) ([]string, error) {
	return sqlGetRecipients(path, "callback")
}
