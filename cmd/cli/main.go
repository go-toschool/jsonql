package main

import (
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "user:pass@tcp(192.168.0.17:3306)/jsonql")
	if err != nil {
		logrus.Fatalf("could not connect to server: %v", err)
	}

	rows, err := db.Query("SELECT * FROM people")
	if err != nil {
		logrus.Fatalf("could not get data: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var firstname, lastname, email, phone string
		if err := rows.Scan(&firstname, &lastname, &email, &phone); err != nil {
			logrus.Fatal(err)
		}
		fmt.Printf("%s\n%s\n%s\n%s\n\n", firstname, lastname, email, phone)
	}
	if err := rows.Err(); err != nil {
		logrus.Fatal(err)
	}
}
