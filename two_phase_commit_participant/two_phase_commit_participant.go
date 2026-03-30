package twophasecommitparticipant

import (
	"context"
	"database/sql"
	"distributedKeyValue/persistence"
	"fmt"
	"log/slog"
)

type TwoPhaseCommitParticipantPersistence interface {
	TryPrepareTransaction(context context.Context, transaction persistence.Transaction, optTx *sql.Tx) (bool, error)
	AbortTransaction(context context.Context, id string, optTx *sql.Tx) error
	CommitTransaction(context context.Context, id string, optTx *sql.Tx) error
	GetWithLockCheck(ctx context.Context, key string) (value string, locked bool, err error)
	GetTransactionStatus(ctx context.Context, transactionId string) (persistence.TransactionState, error)
}

type TwoPhaseCommitParticipant struct {
	PersistenceManager TwoPhaseCommitParticipantPersistence
}

func HandleGetRequest(ctx context.Context, key string, persistenceManager TwoPhaseCommitParticipantPersistence) (string, bool, error) {
	value, locked, err := persistenceManager.GetWithLockCheck(ctx, key)

	if err != nil {
		slog.Error("failed to get value for key", "key", key, "error", err)
		return "", false, fmt.Errorf("failed to get value for key: %w", err)
	}

	if locked {
		slog.Info("key is locked by an active transaction", "key", key)
		return "", true, nil
	}

	return value, false, nil
}

func (twopc *TwoPhaseCommitParticipant) HandlePrepareRequest(context context.Context, transaction persistence.Transaction) (bool, error) {

	// We try to prepare the transaction by inserting it into the database with state "prepared". If there is a conflict with an existing transaction, we return false. If the transaction is successfully prepared, we return true.

	return twopc.PersistenceManager.TryPrepareTransaction(context, transaction, nil)
}

func (twopc *TwoPhaseCommitParticipant) GetTransactionStatus(ctx context.Context, transactionId string) (persistence.TransactionState, error) {

	status, err := twopc.PersistenceManager.GetTransactionStatus(ctx, transactionId)

	if err != nil {
		slog.Error("failed to get transaction status", "transactionId", transactionId, "error", err)
		return "unknown", fmt.Errorf("failed to get transaction status: %w", err)
	}

	return status, nil
}

func (twopc *TwoPhaseCommitParticipant) HandleAckRequest(ctx context.Context, transactionId string, newState persistence.TransactionCoordinatorState) error {

	var err error
	switch newState {
	case persistence.TransactionCoordinatorStateAborted:
		err = twopc.PersistenceManager.AbortTransaction(ctx, transactionId, nil)
	case persistence.TransactionCoordinatorStateCommitted:
		err = twopc.PersistenceManager.CommitTransaction(ctx, transactionId, nil)
	default:
		slog.Error("Received unexpected transaction state", newState)
		return fmt.Errorf("Received unexpected transaction state")
	}

	return err

}
