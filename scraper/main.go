package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	// "log"
	"net/http"
	// "os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type JsonType struct {
	Draw            int `json:"draw"`
	RecordsTotal    int `json:"recordsTotal"`
	RecordsFiltered int `json:"recordsFiltered"`
	Data            []struct {
		ID           int    `json:"id"`
		Nama         string `json:"nama"`
		Nik          string `json:"nik"`
		TempatLahir  string `json:"tempatLahir"`
		JenisKelamin string `json:"jenisKelamin"`
		Tps          int    `json:"tps"`
		Putaran      string `json:"putaran"`
	} `json:"data"`
}

type idNamaType struct {
	Id   int
	Nama string
}

type tpsType struct {
	Id            int
	Nama          int
	KelurahanNama string
}

var (
	jenisKelaminData []idNamaType
	tempatLahirData  []idNamaType
	tpsData          []tpsType
)

func getIdNamaId(idNamaData []idNamaType, nama string) int {
	for _, k := range idNamaData {
		if strings.ToLower(k.Nama) == strings.ToLower(nama) {
			return k.Id
		}
	}
	return 0
}

func getTpsId(tpsData []tpsType, url string) int {
	location := strings.Split(url, "/")
	len := len(location)

	tpsNama := location[len-2]
	tpsInt, _ := strconv.Atoi(tpsNama)
	kelurahanNama := strings.Replace(location[len-3], "%20", " ", -1)
	for _, t := range tpsData {
		if t.Nama == tpsInt && kelurahanNama == t.KelurahanNama {
			return t.Id
		}
	}
	return 0
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	db, _ := sql.Open("sqlite3", "./dpt.db?cache=shared&mode=rwc")

	stmt, _ := db.Prepare(`CREATE TABLE IF NOT EXISTS pemilih (
        id INTEGER NOT NULL,
        nama VARCHAR(100) NOT NULL,
        nik VARCHAR(20) NOT NULL,
        putaran INTEGER NOT NULL,
        jenisKelaminId INTEGER NOT NULL,
        tempatLahirId INTEGER,
        tpsId INTEGER NOT NULL,
        UNIQUE (id, nama, putaran, jenisKelaminId, tempatLahirId, tpsId),
        FOREIGN KEY (jenisKelaminId) REFERENCES jenisKelamin(id),
        FOREIGN KEY (tempatLahirId) REFERENCES tempatLahir(id),
        FOREIGN KEY (tpsId) REFERENCES tps(id)
    )`)
	stmt.Exec()

	stmt, _ = db.Prepare(`CREATE TABLE IF NOT EXISTS jenisKelamin (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nama VARCHAR(100) NOT NULL UNIQUE
    )`)
	stmt.Exec()

	stmt, _ = db.Prepare(`CREATE TABLE IF NOT EXISTS tempatLahir (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nama varchar(100) NOT NULL UNIQUE
    )`)
	stmt.Exec()

	stmt, _ = db.Prepare("INSERT INTO jenisKelamin (nama) VALUES (?)")
	stmt.Exec("laki-laki")
	stmt.Exec("perempuan")

	var (
		urlData       []string
		JsonData      JsonType
		tempatLahirId int64
	)

	rowJenisKelamin, _ := db.Query("SELECT id,nama FROM jenisKelamin")
	for rowJenisKelamin.Next() {
		var jk idNamaType

		rowJenisKelamin.Scan(&jk.Id, &jk.Nama)
		jenisKelaminData = append(jenisKelaminData, jk)
	}
	rowJenisKelamin.Close()

	rowTps, _ := db.Query(`SELECT t.id,t.nama,k.nama kelurahanNama
	    FROM tps t
	    join kelurahan k on k.id=t.kelurahanId`)
	for rowTps.Next() {
		var t tpsType

		rowTps.Scan(&t.Id, &t.Nama, &t.KelurahanNama)
		tpsData = append(tpsData, t)
	}
	rowTps.Close()

	rowUrl, _ := db.Query("SELECT url FROM _source WHERE isFetched=?", 0)
	for rowUrl.Next() {
		var u string

		rowUrl.Scan(&u)
		urlData = append(urlData, u)
	}
	rowUrl.Close()

	for _, url := range urlData {
		res, err := http.Get(url)
		checkErr(err)

		fmt.Println(url)
		json.NewDecoder(res.Body).Decode(&JsonData)
		tpsId := getTpsId(tpsData, url)

		for _, pemilih := range JsonData.Data {
			jenisKelaminId := getIdNamaId(jenisKelaminData, pemilih.JenisKelamin)

			err = db.QueryRow("SELECT id FROM tempatLahir where nama=?", strings.ToUpper(pemilih.TempatLahir)).Scan(&tempatLahirId)
			if err == sql.ErrNoRows {
				fmt.Println("Insert",pemilih.TempatLahir)
				tx, _ := db.Begin()
				stmt, _ = tx.Prepare(`INSERT INTO tempatLahir (nama) VALUES (?)`)
				res, _ := stmt.Exec(strings.ToUpper(pemilih.TempatLahir))
				tempatLahirId, _ = res.LastInsertId()
				err = tx.Commit()
				checkErr(err)
			} else if err != nil {
				panic(err.Error())
			}

			stmt, _ = db.Prepare(`INSERT INTO
				pemilih (id, nama, nik, putaran, jenisKelaminId, tempatLahirId, tpsId)
				VALUES  ( ?,    ?,   ?,       ?,              ?,             ?,     ?)`)
			_, err = stmt.Exec(pemilih.ID, pemilih.Nama, pemilih.Nik, pemilih.Putaran, jenisKelaminId, tempatLahirId, tpsId)

			if err != nil {
				if !strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
					panic(err.Error())
				} else {
					fmt.Println(pemilih.Nik, pemilih.Nama, "already exist")
				}
			} else {
				fmt.Println("Append", pemilih.Nik, pemilih.Nama, "to db")
			}
			// checkErr(err)
		}
		stmt, _ = db.Prepare("UPDATE _source set isFetched=1 where url=?")
		stmt.Exec(url)
		// os.Exit(0)
	}
	db.Close()

}
