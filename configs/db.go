package configs

import (
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func ConnectPostgres() (*sqlx.DB, error) {
	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	user := os.Getenv("PG_USER")
	password := os.Getenv("PG_PASSWORD")
	dbname := os.Getenv("PG_DBNAME")

	if host == "" {
		return nil, fmt.Errorf("missing PG_HOST environment variable")
	}

	if user == "" {
		return nil, fmt.Errorf("missing PG_USER environment variable")
	}

	if password == "" {
		return nil, fmt.Errorf("missing PG_PASSWORD environment variable")
	}

	if dbname == "" {
		return nil, fmt.Errorf("missing PG_DBNAME environment variable")
	}

	if port == "" {
		port = "5432"
	}

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Postgres: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("unable to ping Postgres: %w", err)
	}

	return db, nil
}
