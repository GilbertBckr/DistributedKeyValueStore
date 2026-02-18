package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/glebarez/go-sqlite"
)

var ErrKeyNotFound = errors.New("key not found")

type SqliteDataPersistence struct {
	db *sql.DB
}

func NewSqlitePersistence(connectionString string) (*SqliteDataPersistence, error) {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)", connectionString)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}
	sqliteDB := &SqliteDataPersistence{db: db}
	sqliteDB.InitSchema(context.Background())

	return sqliteDB, nil
}

func (p *SqliteDataPersistence) Close() error {
	return p.db.Close()
}

func (p *SqliteDataPersistence) InitSchema(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, createKeyValueTableString)
	return err
}

func (p *SqliteDataPersistence) Get(ctx context.Context, key string) (string, bool, error) {
	var result string
	query := "SELECT value FROM keyValue WHERE key = ?"

	err := p.db.QueryRowContext(ctx, query, key).Scan(&result)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}
	return result, true, nil
}

func (p *SqliteDataPersistence) Set(ctx context.Context, key string, value string) error {
	insertQuery := `
		INSERT INTO keyValue (key, value)
		VALUES (?, ?)
		ON CONFLICT(key) 
		DO UPDATE SET value = excluded.value;`

	_, err := p.db.ExecContext(ctx, insertQuery, key, value)
	return err
}
