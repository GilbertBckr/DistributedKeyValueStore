package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	_ "modernc.org/sqlite"
)

type TransactionState string

const (
	TransactionStatePrepared  TransactionState = "prepared"
	TransactionStateCommitted TransactionState = "committed"
	TransactionStateAborted   TransactionState = "aborted"
	TransactionStateOblivious TransactionState = "oblivious"
)

type TransactionCoordinatorState string

const (
	TransactionCoordinatorStateWaiting   TransactionCoordinatorState = "waiting"
	TransactionCoordinatorStateAborted   TransactionCoordinatorState = "aborted"
	TransactionCoordinatorStateCommitted TransactionCoordinatorState = "committed"
	TransactionCoordinatorStateCompleted TransactionCoordinatorState = "completed"
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

// Participants is a JSON Object with list of the participants ids and their ack status for the current transaction for the second phase of the 2pc
// This is simply the easiest solution, of course more sophisticated solutions are possible, but this is sufficient for our purposes
const createTransactionManagerTableString = `
	CREATE TABLE IF NOT EXISTS transactionManager (id TEXT PRIMARY KEY, state TEXT NOT NULL, participants TEXT);
`

const createTransactionCoordinatorStateTransitionTrigger = `
	CREATE TRIGGER IF NOT EXISTS enforce_2pc_state_consistency_coordinator
		BEFORE UPDATE OF state ON transactionManager
		FOR EACH ROW
		BEGIN
		    -- Prevent any change if the current state is already terminal
		    SELECT CASE
			WHEN OLD.state = 'committed' AND NEW.state != 'completed' THEN
			    RAISE(ABORT, 'Protocol Violation: Transaction is already COMMITTED and can only be set to COMPLETED.')
			WHEN OLD.state = 'aborted' AND NEW.state != 'completed' THEN
			    RAISE(ABORT, 'Protocol Violation: Transaction is already ABORTED and can only be set to COMPLETED.')
			
			-- Ensure WAITING can only move to the two valid terminal states
			WHEN OLD.state = 'waiting' AND NEW.state NOT IN ('committed', 'aborted') THEN
			    RAISE(ABORT, 'Protocol Violation: WAITING must move to COMMITTED or ABORTED.')
		    END;
		END;
`

const createTransactionStateTransitionTrigger = `
	CREATE TRIGGER IF NOT EXISTS enforce_2pc_state_consistency_participant
		BEFORE UPDATE OF state ON transactions
		FOR EACH ROW
		BEGIN
		    -- Prevent any change if the current state is already terminal
		    SELECT CASE
			WHEN OLD.state = 'committed' AND NEW.state != 'committed' THEN
			    RAISE(ABORT, 'Protocol Violation: Transaction is already COMMITTED.')
			WHEN OLD.state = 'aborted' AND NEW.state != 'aborted' THEN
			    RAISE(ABORT, 'Protocol Violation: Transaction is already ABORTED.')
			
			WHEN NEW.state = 'committed' AND OLD.state != 'prepared' THEN
			    RAISE(ABORT, 'Protocol Violation: Only prepared transactions can be COMMITTED.')
			
			-- Ensure WAITING can only move to the two valid terminal states
			WHEN OLD.state = 'prepared' AND NEW.state NOT IN ('committed', 'aborted') THEN
			    RAISE(ABORT, 'Protocol Violation: prepared must move to COMMITTED or ABORTED.')
		    END;
		END;
`

func MustNewSqliteTransactionManagerPersistence(connectionString string) *SqliteTransactionManagerPersistence {
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&busy_timeout=5000&_txlock=immediate", connectionString)

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
	if err != nil {
		return err
	}

	_, err = p.db.ExecContext(context.Background(), createTransactionCoordinatorStateTransitionTrigger)
	if err != nil {
		return err
	}

	_, err = p.db.ExecContext(context.Background(), createTransactionStateTransitionTrigger)

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

func (p *SqliteTransactionManagerPersistence) CommitTransaction(context context.Context, id string) error {
	_, err := p.db.ExecContext(context, "UPDATE transactions SET state = ? WHERE id = ?", TransactionStateCommitted, id)

	return err
}

// If a transaction with the same id is already present the function returns the state of the existing transaction. If there is a conflict with an existing transaction (i.e. another transaction has already prepared a transaction with the same key), it should return false. If the transaction is successfully prepared, it should return true.
func (p *SqliteTransactionManagerPersistence) TryPrepareTransaction(context context.Context, transaction Transaction, optTx *sql.Tx) (bool, error) {

	var tx *sql.Tx
	var err error
	isLocalTx := false // Tracks whether WE created the transaction

	// 1. Transaction Setup / Injection
	if optTx != nil {
		tx = optTx // Use the caller's transaction
	} else {
		// Create our own transaction
		tx, err = p.db.BeginTx(context, nil)
		if err != nil {
			panic(fmt.Errorf("failed to begin transaction: %w", err))
		}
		isLocalTx = true

		// Guarantee cleanup only if we own the transaction
		defer tx.Rollback()
	}

	// 2. Check for existing transaction
	var existingState TransactionState
	err = tx.QueryRowContext(context, `SELECT state FROM transactions WHERE id = ?`, transaction.Id).Scan(&existingState)

	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("failed to check for existing transaction: %w", err))
	}
	if err == nil { // err == nil means a row was found
		if existingState == TransactionStatePrepared {
			return true, nil
		}
		if existingState == TransactionStateAborted {
			return false, nil
		}
		panic(fmt.Sprintf("Found unexpected state for prepare request with id %s, found state: %s", transaction.Id, existingState))
	}

	// 3. Check if key is locked by any other transaction
	checkKeyQuery := `
		SELECT state FROM transactions WHERE key = ? AND state = ? ORDER BY seq_num DESC LIMIT 1
	`
	var state TransactionState
	err = tx.QueryRowContext(context, checkKeyQuery, transaction.Key, TransactionStatePrepared).Scan(&state)

	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("failed to check if key %s is locked by any transaction: %w", transaction.Key, err))
	}

	// 4. Branching Logic based on the lock check
	if err == sql.ErrNoRows {
		// Insert the transaction with state "prepared"
		insertQuery := `
			INSERT INTO transactions (id, state, key, value) VALUES (?, ?, ?, ?)
		`
		_, err := tx.ExecContext(context, insertQuery, transaction.Id, TransactionStatePrepared, transaction.Key, transaction.Value)
		if err != nil {
			panic(fmt.Errorf("failed to insert transaction with id %s: %w", transaction.Id, err))
		}

		// 5a. Commit only if we created the transaction
		if isLocalTx {
			if err = tx.Commit(); err != nil {
				panic(fmt.Errorf("failed to commit prepared transaction with id %s: %w", transaction.Id, err))
			}
		}
		return true, nil

	} else {
		// Key is locked by another TryPrepareTransaction.
		abortQuery := `
			INSERT INTO transactions (id, state, key, value) VALUES (?, ?, ?, ?)
		`
		_, err := tx.ExecContext(context, abortQuery, transaction.Id, TransactionStateAborted, transaction.Key, transaction.Value)
		if err != nil {
			panic(fmt.Errorf("could not abort transaction with id %s: %w", transaction.Id, err))
		}

		// 5b. Commit the aborted state only if we created the transaction
		if isLocalTx {
			if err = tx.Commit(); err != nil {
				panic(fmt.Errorf("failed to commit aborted transaction with id %s: %w", transaction.Id, err))
			}
		}
		return false, nil
	}
}

func (p *SqliteTransactionManagerPersistence) TransactionCoordinatorStartTransaction(context context.Context, transaction Transaction, participants []ParticpantDB) (bool, error) {

	queryInsertTransactionCoordinatorEntry := `
		INSERT INTO transactionManager (id, state, participants ) VALUES (?, ?, ?);
	`
	jsonParticipants, err := json.Marshal(participants)
	if err != nil {
		panic(fmt.Errorf("failed to marshal participants for transaction coordinator entry: %w", err))
	}

	tx, err := p.db.BeginTx(context, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})

	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("failed to begin transaction for inserting transaction coordinator entry: %w", err))
	}

	// check if transaction can be inserted
	canCommit, err := p.TryPrepareTransaction(context, transaction, tx)

	if err != nil {
		tx.Rollback()
		panic(fmt.Errorf("failed to prepare transaction for inserting transaction coordinator entry: %w", err))
	}

	if !canCommit {
		_, err = tx.ExecContext(context, queryInsertTransactionCoordinatorEntry, transaction.Id, TransactionCoordinatorStateAborted, jsonParticipants)
	} else {
		_, err = tx.ExecContext(context, queryInsertTransactionCoordinatorEntry, transaction.Id, TransactionCoordinatorStateWaiting, jsonParticipants)
	}

	if err != nil {
		tx.Rollback()
		panic(fmt.Errorf("failed to insert transaction coordinator entry: %w", err))
	}

	err = tx.Commit()
	if err != nil {
		panic(fmt.Errorf("failed to commit transaction for inserting transaction coordinator entry: %w", err))
	}

	return canCommit, nil
}

func (p *SqliteTransactionManagerPersistence) SetTransactionCoordinatorAndOwnParticipantState(context context.Context, id string, state TransactionCoordinatorState) error {

	// We need to update the state of the transaction coordinator entry and also the state of our own participant entry in the transactions table. We can do this in a single transaction to ensure consistency.

	tx, err := p.db.BeginTx(context, &sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: false})

	if err != nil {
		return fmt.Errorf("failed to begin transaction for updating transaction coordinator and own participant state for transaction with id %s: %w", id, err)
	}
	defer tx.Rollback()

	// Update the transaction coordinator state
	err = p.SetTransactionCoordinatorState(context, id, state, tx)

	if err != nil {
		return fmt.Errorf("failed to update transaction coordinator state for transaction with id %s: %w", id, err)
	}

	// Update the own participant state in the transactions table
	var participantState TransactionState

	switch state {
	case TransactionCoordinatorStateAborted:
		participantState = TransactionStateAborted
	case TransactionCoordinatorStateCommitted:
		participantState = TransactionStateCommitted
	default:
		panic(fmt.Sprintf("unexpected transaction coordinator state %s for transaction with id %s when trying to set own participant state", state, id))
	}

	updateParticipantQuery := `
		UPDATE transactions SET state = ? WHERE id = ?
	`

	_, err = tx.ExecContext(context, updateParticipantQuery, participantState, id)

	if err != nil {
		return fmt.Errorf("failed to update own participant state for transaction with id %s: %w", id, err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for updating transaction coordinator and own participant state for transaction with id %s: %w", id, err)
	}

	return nil

}

func (p *SqliteTransactionManagerPersistence) SetTransactionCoordinatorState(context context.Context, id string, state TransactionCoordinatorState, optTx *sql.Tx) error {
	updateQuery := `
		UPDATE transactionManager SET state = ? WHERE id = ?;
	`
	var tx *sql.Tx
	var err error
	isLocalTx := false

	if optTx != nil {
		tx = optTx
	} else {
		tx, err = p.db.BeginTx(context, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for updating transaction coordinator state for transaction with id %s: %w", id, err)
		}
		isLocalTx = true
		defer tx.Rollback()
	}

	_, err = tx.ExecContext(context, updateQuery, state, id)

	if err != nil {
		return fmt.Errorf("failed to update transaction coordinator state for transaction with id %s: %w", id, err)
	}

	if isLocalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction for updating transaction coordinator state for transaction with id %s: %w", id, err)
		}
	}

	return nil
}

func (p *SqliteTransactionManagerPersistence) GetTransactionsInPhase1(context context.Context) ([]TransactionAndParticipants, error) {
	query := `SELECT tm.id, t.key, t.value, tm.participants FROM transactionManager AS tm
		JOIN transactions AS t ON tm.id = t.id
		WHERE tm.state = ?`

	rows, err := p.db.QueryContext(context, query, TransactionCoordinatorStateWaiting)

	if err != nil {
		panic(fmt.Errorf("failed to query transactions in phase 1: %w", err))
	}

	defer rows.Close()

	var transactions []TransactionAndParticipants

	for rows.Next() {

		var transaction = TransactionAndParticipants{}
		var participantString string

		err := rows.Scan(&transaction.Transaction.Id, &transaction.Transaction.Key, &transaction.Transaction.Value, &participantString)

		if err != nil {
			slog.Error("failed to scan transaction in phase 1", "error", err)
		}

		err = json.Unmarshal([]byte(participantString), &transaction.Participants)

		if err != nil {
			slog.Error("failed to unmarshal participants for transaction in phase 1", "error", err)
		}

		transactions = append(transactions, transaction)
	}
	return transactions, nil

}
