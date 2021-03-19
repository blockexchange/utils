package main

type SchemaPart struct {
	ID       int64  `db:"id"`
	Data     []byte `db:"data"`
	MetaData []byte `db:"metadata"`
}

func main() {
	//TODO: setup db connection
	//TODO: query existing schemaparts
	//TODO: convert each from gzip to deflate compression
	//TODO: insert re-compressed data and metadata again

}
