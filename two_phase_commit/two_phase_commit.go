package twophasecommit

import (
	"context"
	"distributedKeyValue/persistence"
	servicediscovery "distributedKeyValue/service_discovery"

	"github.com/google/uuid"
)

type TwoPhaseCommit struct {
	PersistenceManager persistence.TransactionManagerPersistence
	SDiscovery         servicediscovery.ServiceDiscovery
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
			Participant: participant,
			State:       persistence.TransactionStateOblivious,
		}
	}

	// We persist the transaction here to the database with state "prepared" and the list of participants. This is necessary to be able to recover from failures during the commit phase.
	// Next the scheduler can pick up this transaction perform phase 1 of this transaction
	couldCommit, err := twopc.PersistenceManager.TransactionCoordinatorStartTransaction(context, transaction, participantsWithState)

	return couldCommit, transactionId, err
}
