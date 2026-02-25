package twophasecommitcoordinator

import (
	"context"
	"distributedKeyValue/persistence"
	servicediscovery "distributedKeyValue/service_discovery"

	"github.com/google/uuid"
)

type twoPhaseCommitPersistence interface {
	TransactionCoordinatorStartTransaction(context context.Context, transaction persistence.Transaction, participants []persistence.ParticpantDB) (bool, error)
}

type serviceDiscoveryStartTransaction interface {
	GetParticipants() []servicediscovery.Participant
}

type TwoPhaseCommit struct {
	PersistenceManager twoPhaseCommitPersistence
	SDiscovery         serviceDiscoveryStartTransaction
}

func getUniqueTransactionId() string {
	// using UUID here can later migrate to more reasonable solution as hybrid per node logical clock etc.

	return uuid.New().String()

}

func (twopc *TwoPhaseCommit) StartNewTransaction(context context.Context, key string, value string) (bool, string, error) {
	transactionId := getUniqueTransactionId()

	transaction := persistence.Transaction{
		Id:    transactionId,
		Key:   key,
		Value: value,
	}

	participants := twopc.SDiscovery.GetParticipants()
	participantsWithState := make([]persistence.ParticpantDB, len(participants))
	for i, participant := range participants {
		participantsWithState[i] = persistence.ParticpantDB{
			ID:        participant.ID,
			ResultAck: false,
		}
	}

	// We persist the transaction here to the database with state "prepared" and the list of participants. This is necessary to be able to recover from failures during the commit phase.
	// Next the scheduler can pick up this transaction perform phase 1 of this transaction
	couldCommit, err := twopc.PersistenceManager.TransactionCoordinatorStartTransaction(context, transaction, participantsWithState)

	return couldCommit, transactionId, err
}
