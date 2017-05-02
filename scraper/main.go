package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	// "fmt"
	// "strconv"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	database, _ := sql.Open("sqlite3", "./dpt.db")

	statement, _ := database.Prepare(`CREATE TABLE IF NOT EXISTS pemilih (
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
	statement.Exec()

	statement, _ = database.Prepare(`CREATE TABLE IF NOT EXISTS jenisKelamin (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nama VARCHAR(100) NOT NULL UNIQUE
    )`)
	statement.Exec()

	statement, _ = database.Prepare(`CREATE TABLE IF NOT EXISTS tempatLahir (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nama varchar(100) NOT NULL UNIQUE
    )`)
	statement.Exec()

	statement, _ = database.Prepare("INSERT INTO jenisKelamin (nama) VALUES (?)")
	statement.Exec("laki-laki")
	statement.Exec("perempuan")

	// rows, _ := database.Query("SELECT id, firstname, lastname FROM people")
	// var id int
	// var firstname string
	// var lastname string
	// for rows.Next() {
	//     rows.Scan(&id, &firstname, &lastname)
	//     fmt.Println(strconv.Itoa(id) + ": " + firstname + " " + lastname)
	// }
	// type JsonTypePemilih
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
	var (
		url      string
		JsonData JsonType
	)

	urlData, _ := database.Query("SELECT url FROM _source WHERE isFetched=?", 0)
	for urlData.Next() {
		urlData.Scan(&url)
		r, err := http.Get(url)
		if err != nil {
			panic(err.Error())
		}

		log.Println(url)
		err = json.NewDecoder(r.Body).Decode(&JsonData)
		if err != nil {
			panic(err.Error())
		}
		tps := JsonData.Data
		for _, pemilih := range tps {
			log.Println(pemilih)

			statement, _ = database.Prepare(`INSERT INTO
				pemilih (id, nama, nik, putaran, jenisKelaminId, tempatLahirId, tpsId)
				VALUES  ( ?,    ?,   ?,       ?,              ?,             ?,     ?)`)
			statement.Exec(pemilih.ID, pemilih.Nama, pemilih.Nik, pemilih.Putaran, pemilih.JenisKelamin, pemilih.Tps)
		}
		os.Exit(0)
	}

}
