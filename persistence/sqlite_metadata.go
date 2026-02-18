package persistence

import (
	"context"
	"database/sql"
	"fmt"
)

type SqliteMetadata struct {
	db *sql.DB
}

func (m *SqliteMetadata) MustInitSchema() {
	createTransactionsTableString := `
	CREATE TABLE IF NOT EXISTS transactions (id TEXT PRIMARY KEY, state TEXT NOT NULL);
	`
	_, err := m.db.Exec(createTransactionsTableString)

	if err != nil {
		panic(err)
	}
}

func (m *SqliteMetadata) MarkTransactionAsPrepared(id string) error {
	_, err := m.db.Exec("INSERT OR REPLACE INTO transactions (id, state) VALUES (?, ?)", id, "prepared")

	return err
}

func (m *SqliteMetadata) AbortTransaction(id string) error {
	_, err := m.db.Exec("INSERT OR REPLACE INTO transactions (id, state) VALUES (?, ?)", id, "aborted")

	return err
}

func (m *SqliteMetadata) CommitTransaction(id string) error {
	_, err := m.db.Exec("INSERT OR REPLACE INTO transactions (id, state) VALUES (?, ?)", id, "committed")

	return err
}

func (m *SqliteMetadata) GetTransactionState(id string) (string, bool, error) {
	var state string
	query := "SELECT state FROM transactions WHERE id = ?"

	err := m.db.QueryRowContext(context.Background(), query, id).Scan(&state)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	return state, true, nil
}

func MustNewSqliteMetadata(connectionString string) *SqliteMetadata {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)", connectionString)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		panic(err)
	}

	if err := db.Ping(); err != nil {
		panic(err)
	}
	sqliteDB := &SqliteMetadata{db: db}
	sqliteDB.MustInitSchema()

	return sqliteDB
}
