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

func sqlAddEmail(path, email string) error {
	// open database
	db, err := sqlOpenDatabase(path)
	if err != nil {
		logger.Printf("[sql] failed to open requests database when adding email: [%s]", err.Error())
		return errors.New("Failed to open requests database")
	}
	defer db.Close()

	// attempt to create table, even if it exists
	sqlStmt := `create table if not exists ocr_requests (email text);`
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
	stmt, err := tx.Prepare("insert into ocr_requests(email) values(?)")
	if err != nil {
		logger.Printf("[sql] failed to prepare transaction: [%s]", err.Error())
		return errors.New("Failed to prepare request transaction")
	}
	defer stmt.Close()
	_, err = stmt.Exec(email)
	if err != nil {
		logger.Printf("[sql] failed to execute transaction: [%s]", err.Error())
		return errors.New("Failed to execute request transaction")
	}
	tx.Commit()

	return nil
}

func sqlGetEmails(path string) ([]string, error) {
	// open database
	db, err := sqlOpenDatabase(path)
	if err != nil {
		logger.Printf("[sql] failed to open requests database when adding email: [%s]", err.Error())
		return nil, errors.New("Failed to open requests database")
	}
	defer db.Close()

	// grab unique emails

	var emails []string

	rows, err := db.Query("select email from ocr_requests;")
	if err != nil {
		logger.Printf("[sql] failed to retrieve emails: [%s]", err.Error())
		return nil, errors.New("Failed to retrieve requestor emails")
	}
	defer rows.Close()

	for rows.Next() {
		var email string
		err = rows.Scan(&email)
		if err != nil {
			logger.Printf("[sql] failed to scan email: [%s]", err.Error())
			return nil, errors.New("Failed to scan email address")
		}

		emails = appendStringIfMissing(emails, email)
	}

	// check for errors

	err = rows.Err()
	if err != nil {
		logger.Printf("[sql] select query failed: [%s]", err.Error())
		return nil, errors.New("Failed to select requestor emails")
	}

	return emails, nil
}
