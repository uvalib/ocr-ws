package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func determinePidType(pid string) (pidType string) {
	var cnt int

	pidType = "invalid"

	qs := "select count(*) as cnt from metadata b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)

	if cnt == 1 {
		pidType = "metadata"
		return
	}

	qs = "select count(*) as cnt from master_files b where pid=?"
	db.QueryRow(qs, pid).Scan(&cnt)

	if cnt == 1 {
		pidType = "master_file"
		return
	}

	return
}

func getMasterFilePages(pid string, w http.ResponseWriter) (pages []pageInfo, err error) {
	var pg pageInfo
	var origID sql.NullInt64
	qs := `select pid, filename, title, original_mf_id from master_files where pid = ?`
	err = db.QueryRow(qs, pid).Scan(&pg.PID, &pg.Filename, &pg.Title, &origID)
	if err != nil {
		logger.Printf("Request failed: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
		return nil, err
	}

	// if this is a clone, grab the info for the original
	if origID.Valid {
		qs := `select pid, filename, title from master_files where id = ?`
		err = db.QueryRow(qs, origID.Int64).Scan(&pg.PID, &pg.Filename, &pg.Title)
		if err != nil {
			logger.Printf("Request failed: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
			return nil, err
		}
	}

	pages = append(pages, pg)

	return pages, nil
}

// NOTE: when called from Tracksys, the unitID will be set. Honor this and generate OCR
// of all masterfiles in that unit regardless of published status. When called from Virgo,
// unitID will NOT be set. Run through all units and only include those that are
// in the DL and are publicly visible
//
func getMetadataPages(pid string, w http.ResponseWriter, unitID int, ocrPages string) (pages []pageInfo, err error) {
	// Get metadata for the passed PID
	logger.Printf("Get Metadata pages params: PID: %s, Unit %d, Pages: %s", pid, unitID, ocrPages)

	var availability sql.NullInt64
	var metadataID int
	var title string
	var ocrLanguage sql.NullString
	qs := "select id, title, availability_policy_id, ocr_language_hint from metadata where pid=?"
	err = db.QueryRow(qs, pid).Scan(&metadataID, &title, &availability, &ocrLanguage)
	if err != nil {
		logger.Printf("Request failed: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Unable to OCR: %s", err.Error())
		return nil, err
	}

	// Must have availability set
	if availability.Valid == false && config.allowUnpublished.value == false {
		logger.Printf("%s not found", pid)
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "%s not found", pid)
		return nil, errors.New("PID does not have availability set")
	}

	// Get data for all master files from units associated with metadata / unit
	// Do this in two passes, once for orignal master files and once for clones
	for i := 0; i < 2; i++ {
		queryParams := []interface{}{metadataID}
		if i == 0 {
			// Non-cloned master files
			qs = `select m.id, m.pid, m.filename, m.title from master_files m
               inner join units u on u.id = m.unit_id
               where m.metadata_id = ? and u.include_in_dl = 1 and m.original_mf_id is null`
			if unitID > 0 {
				qs = `select m.id, m.pid, m.filename, m.title from master_files m
                  where unit_id = ? and m.original_mf_id is null`
				queryParams = []interface{}{unitID}
			}
		} else {
			// Cloned master files
			qs = `select om.id, om.pid, om.filename, om.title from master_files m
			      inner join master_files om on om.id = m.original_mf_id
               inner join units u on u.id = m.unit_id
			      where m.metadata_id = ? and u.include_in_dl = 1 and m.original_mf_id is not null`
			if unitID > 0 {
				qs = `select om.id, om.pid, om.filename, om.title from master_files m
   			      inner join master_files om on om.id = m.original_mf_id
   			      where m.unit_id = ? and m.original_mf_id is not null`
				queryParams = []interface{}{unitID}
			}
		}

		// Filter to only pages requested?
		if len(ocrPages) > 0 {
			idStr := strings.Split(ocrPages, ",")
			for _, val := range idStr {
				id, invalid := strconv.Atoi(val)
				if invalid == nil {
					queryParams = append(queryParams, id)
				}
			}
			qs = qs + " and m.id in (?" + strings.Repeat(",?", len(idStr)-1) + ")"
		}

		logger.Printf("Query: %s, Params: %s", qs, queryParams)
		rows, queryErr := db.Query(qs, queryParams...)
		defer rows.Close()
		if queryErr != nil {
			logger.Printf("Request failed: %s", queryErr.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Unable to OCR: %s", queryErr.Error())
			return nil, err
		}

		for rows.Next() {
			var pg pageInfo
			var mfID int
			err = rows.Scan(&mfID, &pg.PID, &pg.Filename, &pg.Title)
			if ocrLanguage.Valid {
				if len(ocrLanguage.String) == 3 {
					pg.lang = ocrLanguage.String
				}
			}
			if err != nil {
				logger.Printf("Unable to retrieve MasterFile data for OCR generation %s: %s", pid, err.Error())
				continue
			}

			pages = append(pages, pg)
		}
	}
	return pages, nil
}
