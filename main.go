package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
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
	w := zlib.NewWriter(buf)

	_, err = io.Copy(w, r)
	if err != nil {
		return nil, err
	}
	w.Close()

	return buf.Bytes(), nil
}

func Gunzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer([]byte{})

	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Deflate(data []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	w := zlib.NewWriter(buf)
	r := bytes.NewReader(data)

	_, err := io.Copy(w, r)
	if err != nil {
		return nil, err
	}
	w.Close()

	return buf.Bytes(), nil
}

func ConvertMapblockData(data []byte) {
	if len(data)%4 != 0 {
		panic("mapblock data not aligned")
	}
	size := len(data) / 4
	for i := 0; i < size; i++ {
		node_id_low := data[(i*2)+0]
		node_id_high := data[(i*2)+1]
		param1 := data[(2*size)+i]
		param2 := data[(3*size)+i]

		var node_id = uint(node_id_low) + (uint(node_id_high) * 256)
		node_id += 32768

		node_id_low_new := byte(node_id % 256)
		node_id_high_new := byte((node_id - uint(node_id_low_new)) / 256)

		data[(i*2)+0] = node_id_high_new
		data[(i*2)+1] = node_id_low_new

		data[(2*size)+i] = param1 + 0x80
		data[(3*size)+i] = param2 + 0x80
	}
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

		data, err := Gunzip(schemapart.Data)
		if err != nil {
			panic(err)
		}
		ConvertMapblockData(data)
		deflated_data, err := Deflate(data)
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
		_, err = DB.Exec(query, schemapart.ID, deflated_data, metadata)
		if err != nil {
			panic(err)
		}

	}

}
