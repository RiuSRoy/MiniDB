package minidb

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/RiuSRoy/MiniDB/engine"
	"github.com/RiuSRoy/MiniDB/server"
)

func main() {
	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	db, err := engine.Open(dataDir)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	srv := server.New(db)

	fmt.Println("minidb listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", srv))
}
