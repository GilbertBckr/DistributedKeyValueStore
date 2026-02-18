package persistence

import (
	"context"
	"database/sql"
	"fmt"

	"golang.org/x/tools/go/analysis/checker"
)

type TransactionState string

const (
	TransactionStatePrepared  TransactionState = "prepared"
	TransactionStateCommitted TransactionState = "committed"
	TransactionStateAborted   TransactionState = "aborted"
)

type SqliteTransactionManagerPersistence struct {
	db *sql.DB
}

const createKeyValueTableString = `
	CREATE TABLE IF NOT EXISTS keyValue (key TEXT PRIMARY KEY, value TEXT NOT NULL);
`

const createTransactionsTableString = `
CREATE TABLE IF NOT EXISTS transactions (
    seq_num INTEGER PRIMARY KEY AUTOINCREMENT, -- Strict, non-reusing counter
    id TEXT UNIQUE NOT NULL,
    state TEXT NOT NULL, 
    key TEXT, 
    value TEXT
);
`

const createTransactionManagerTableString = `
	CREATE TABLE IF NOT EXISTS transactionManager (id TEXT PRIMARY KEY, state TEXT NOT NULL, otherNodes TEXT);
`

func MustNewSqliteTransactionManagerPersistence(connectionString string) *SqliteTransactionManagerPersistence {
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(WAL)", connectionString)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		panic(err)
	}

	persistence := &SqliteTransactionManagerPersistence{db: db}

	err = persistence.InitSchema()

	if err != nil {
		panic(err)
	}

	return &SqliteTransactionManagerPersistence{db: db}
}

func (p *SqliteTransactionManagerPersistence) Close() error {
	return p.db.Close()
}

func (p *SqliteTransactionManagerPersistence) InitSchema() error {
	_, err := p.db.ExecContext(context.Background(), createKeyValueTableString)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(context.Background(), createTransactionsTableString)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(context.Background(), createTransactionManagerTableString)
	return err
}

func (p *SqliteTransactionManagerPersistence) Get(context context.Context, key string) (string, bool, error) {
	var result string
	var state TransactionState

	// check if key is locked by any transaction, if so return err

	query := `
	SELCT value, state FROM keyValue WHERE key = ?
	JOIN transactions ON keyValue.key = transactions.key ORDER BY transactions.seq_num DESC LIMIT 1
	`

	err := p.db.QueryRowContext(context, query, key).Scan(&result, &state)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}

	if state == TransactionStatePrepared {
		return "", true, fmt.Errorf("key is locked by a prepared transaction")
	}
	return result, true, nil
}

func (p *SqliteTransactionManagerPersistence) GetTransactionState(context context.Context, id string) (TransactionState, bool, error) {

	var state TransactionState

	query := "SELECT state FROM transactions WHERE id = ?"

	err := p.db.QueryRowContext(context, query, id).Scan(&state)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}

	return state, true, nil
}

func (p *SqliteTransactionManagerPersistence) AbortTransaction(context context.Context, id string) error {
	_, err := p.db.ExecContext(context, "INSERT OR REPLACE INTO transactions (id, state) VALUES (?, ?)", id, TransactionStateAborted)

	return err
}

func (p *SqliteTransactionManagerPersistence) TryPerpareTransaction(context context.Context, transaction Transaction) (bool, error) {

	state, present, err := p.GetTransactionState(context, transaction.id)

	if err != nil {
		return false, err
	}

	if present {
		panic(fmt.Sprintf("transaction with id %s already exists with state %s", transaction.id, state))
	}

	// Check if key is locked by any other transaction, if so return false

	checkKeyQuery := `
		SELECT state FROM transactions WHERE key = ? AND state = ? ORDER BY seq_num DESC LIMIT 1
	`

	err = p.db.QueryRowContext(context, checkKeyQuery, transaction.key, TransactionStatePrepared).Scan(&state)

	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("failed to check if key %s is locked by any transaction: %w", transaction.key, err))
	}

	if err == sql.ErrNoRows {
		// Insert the transaction with state "prepared"
		insertQuery := `
		INSERT INTO transactions (id, state, key, value) VALUES (?, ?, ?, ?)
		`
		_, err := p.db.ExecContext(context, insertQuery, transaction.id, TransactionStatePrepared, transaction.key, transaction.value)
		if err != nil {
			panic(fmt.Errorf("failed to insert transaction with id %s: %w", transaction.id, err))
		}
		return true, nil
	} else {
		// key is locked by another TryPerpareTransaction
		// abort the transaction and return false
		p.AbortTransaction(context, transaction.id)
		return false, nil
	}

}
