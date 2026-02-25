package twophasecommitparticipant

import (
	"context"
	"database/sql"
	"distributedKeyValue/persistence"
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
