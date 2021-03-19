package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type SchemaPart struct {
	ID       int64  `db:"id"`
	SchemaID int64  `db:"schema_id"`
	OffsetX  int    `db:"offset_x"`
	OffsetY  int    `db:"offset_y"`
	OffsetZ  int    `db:"offset_z"`
	Mtime    int64  `db:"mtime"`
	Data     []byte `db:"data"`
	MetaData []byte `db:"metadata"`
}

func GzipToDeflate(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer([]byte{})
	w, err := flate.NewWriter(buf, 3)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(w, r)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func main() {
	// setup db connection
	connStr := fmt.Sprintf(
		"user=%s password=%s port=%s host=%s dbname=%s sslmode=disable",
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGPORT"),
		os.Getenv("PGHOST"),
		os.Getenv("PGDATABASE"))

	log.Printf("Connecting to %s", connStr)
	var err error
	DB, err := sqlx.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	// query existing schemaparts
	list := []SchemaPart{}
	query := `select * from schemapart`
	err = DB.Select(&list, query)

	if err != nil {
		panic(err)
	}

	log.Printf("found %d schema parts", len(list))

	// convert each from gzip to deflate compression
	for _, schemapart := range list {
		log.Printf("Converting part id=%d", schemapart.ID)

		data, err := GzipToDeflate(schemapart.Data)
		if err != nil {
			panic(err)
		}

		metadata, err := GzipToDeflate(schemapart.MetaData)
		if err != nil {
			panic(err)
		}

		query = `
			update schemapart
			set data = $2, metadata = $3
			where id = $1
		`
		// insert re-compressed data and metadata again
		_, err = DB.Exec(query, schemapart.ID, data, metadata)
		if err != nil {
			panic(err)
		}

	}

}
