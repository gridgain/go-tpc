package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/alexbrainman/odbc"
)

func main() {
	// DSN-less connection string for GridGain 9 ODBC
	connStr := "Driver={GridGain 9};Address=127.0.0.1:10800;"

	db, err := sql.Open("odbc", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping: %v", err)
	}
	fmt.Println("Connected to GridGain 9 via ODBC")

	rows, err := db.Query("SELECT * FROM warehouse")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	fmt.Printf("Columns: %v\n", cols)

	values := make([]interface{}, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
		rowCount++
		for i, col := range cols {
			v := values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			fmt.Printf("  %s: %v\n", col, v)
		}
		fmt.Println("---")
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("Rows error: %v", err)
	}
	fmt.Printf("Total rows: %d\n", rowCount)
}
