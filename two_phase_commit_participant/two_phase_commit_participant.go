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
	AbortTransaction(context context.Context, id string) error
	CommitTransaction(context context.Context, id string) error
}

type TwoPhaseCommitParticipant struct {
	PersistenceManager TwoPhaseCommitParticipantPersistence
}

func (twopc *TwoPhaseCommitParticipant) HandlePrepareRequest(context context.Context, transaction persistence.Transaction) (bool, error) {

	// We try to prepare the transaction by inserting it into the database with state "prepared". If there is a conflict with an existing transaction, we return false. If the transaction is successfully prepared, we return true.

	return twopc.PersistenceManager.TryPrepareTransaction(context, transaction, nil)
}

func (twopc *TwoPhaseCommitParticipant) HandleAckRequest(ctx context.Context, transactionId string, newState persistence.TransactionCoordinatorState) error {

	var err error
	switch newState {
	case persistence.TransactionCoordinatorStateAborted:
		err = twopc.PersistenceManager.AbortTransaction(ctx, transactionId)
	case persistence.TransactionCoordinatorStateCommitted:
		err = twopc.PersistenceManager.CommitTransaction(ctx, transactionId)
	default:
		slog.Error("Received unexpected transaction state", newState)
		return fmt.Errorf("Received unexpected transaction state")
	}

	return err

}
