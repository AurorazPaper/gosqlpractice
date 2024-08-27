package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
)

const (
	host     = "database-1.c34m4uc2y25o.us-east-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	password = "Revolver64"
	dbname   = "postgres"
)


func main() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully connected!")

    // Call the function to process the gender data
    if err := processGenderData(db); err != nil {
        log.Fatal("Error processing gender data:", err)
    }
	/*
	// Query to execute
	query := "SELECT * FROM mockdata"

	// Prepare the query
	rows, err := db.Query(query) // Example query to search for males
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate through the rows and print results
	for rows.Next() {
		var id int
		var firstName, lastName, email, gender, ipAddress string

		// Scan the result into variables
		err := rows.Scan(&id, &firstName, &lastName, &email, &gender, &ipAddress)
		if err != nil {
			log.Fatal(err)
		}

		// Print the result
		fmt.Printf("ID: %d, First Name: %s, Last Name: %s, Email: %s, Gender: %s, IP Address: %s\n",
			id, firstName, lastName, email, gender, ipAddress)
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	*/
}

// processGenderData performs the tasks of creating the table, inserting data, and verifying results.
func processGenderData(db *sql.DB) error {
    // Step 1: Create the new table
    createTableQuery := `
        CREATE TABLE IF NOT EXISTS grouped_gender_data (
            id SERIAL PRIMARY KEY,
            original_gender TEXT,
            grouped_gender TEXT,
            CONSTRAINT unique_gender_pair UNIQUE (original_gender, grouped_gender)
        );
    `
    _, err := db.Exec(createTableQuery)
    if err != nil {
        return fmt.Errorf("error creating table: %v", err)
    }

    // Step 2: Insert grouped data into the new table
    insertDataQuery := `
        INSERT INTO grouped_gender_data (original_gender, grouped_gender)
        SELECT
            gender AS original_gender,
            CASE
                WHEN LOWER(gender) IN ('male', 'm') THEN 'male'
                WHEN LOWER(gender) IN ('female', 'f') THEN 'female'
                ELSE 'other'
            END AS grouped_gender
        FROM mockdata
        ON CONFLICT (original_gender, grouped_gender) DO NOTHING; -- Handle duplicates based on the unique constraint
    `
    _, err = db.Exec(insertDataQuery)
    if err != nil {
        return fmt.Errorf("error inserting data: %v", err)
    }

    // Step 3: Verify the new table data
    rows, err := db.Query("SELECT * FROM grouped_gender_data")
    if err != nil {
        return fmt.Errorf("error querying new table: %v", err)
    }
    defer rows.Close()

    // Print results
    for rows.Next() {
        var id int
        var originalGender, groupedGender string
        if err := rows.Scan(&id, &originalGender, &groupedGender); err != nil {
            return fmt.Errorf("error scanning row: %v", err)
        }
        fmt.Printf("ID: %d, Original Gender: %s, Grouped Gender: %s\n", id, originalGender, groupedGender)
    }
    if err := rows.Err(); err != nil {
        return fmt.Errorf("error iterating over rows: %v", err)
    }

    return nil
}
