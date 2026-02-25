package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/alexbrainman/odbc"
)

func main() {
	connStr := "Driver={GridGain 9};Address=127.0.0.1:10800;"
	db, err := sql.Open("odbc", connStr)
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	db.SetMaxIdleConns(2)

	queries := []string{
		"SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
		"SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ?",
		"UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?",
		"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
		"SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?",
		"SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC FETCH FIRST 1 ROWS ONLY",
	}

	// Mimic go-tpc: multiple goroutines with connections + stmts, then concurrent cleanup
	var wg sync.WaitGroup
	for t := 0; t < 4; t++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			conn, err := db.Conn(context.Background())
			if err != nil {
				log.Printf("Thread %d: Conn failed: %v", tid, err)
				return
			}
			myStmts := make([]*sql.Stmt, len(queries))
			for j, q := range queries {
				myStmts[j], err = conn.PrepareContext(context.Background(), q)
				if err != nil {
					log.Printf("Thread %d: Prepare %d failed: %v", tid, j, err)
					return
				}
			}
			// Execute some queries
			for i := 0; i < 10; i++ {
				rows, err := myStmts[0].Query(1, 6, "ABLEESEABLE")
				if err != nil {
					continue
				}
				for rows.Next() {}
				rows.Close()
			}
			// Cleanup like go-tpc CleanupThread
			for _, s := range myStmts { s.Close() }
			conn.Close()
			fmt.Printf("Thread %d: cleaned up\n", tid)
		}(t)
	}
	wg.Wait()
	fmt.Println("All threads done")

	fmt.Println("Calling db.Close()...")
	db.Close()
	fmt.Println("db.Close() succeeded")
	time.Sleep(200 * time.Millisecond)
	fmt.Println("Done.")
}
