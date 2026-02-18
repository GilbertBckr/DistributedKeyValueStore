package twophasecommit

import (
	"distributedKeyValue/locking"
	"distributedKeyValue/persistence"
	servicediscovery "distributedKeyValue/service_discovery"

	"github.com/google/uuid"
)

type TwoPhaseCommit struct {
	persistenceManager persistence.TransactionManagerPersistence
	sDiscovery         servicediscovery.ServiceDiscovery
	locking            locking.Locking
}

func getUniqueTransactionId() string {
	// using UUID here can later migrate to more reasonable solution as hybrid per node logical clock etc.

	return uuid.New().String()

}

func (twopc *TwoPhaseCommit) StartNewTransaction(key string, value string) error {
	transactionId := getUniqueTransactionId()

	// check if can apply on local node, if not return error to client
	err := twopc.locking.Lock(key)
	if err != nil {
		return err
	}

	// TODO: parrallelize this part

	particpants := twopc.sDiscovery.GetParticipants()

	for _, participant := range particpants {
		// send prepare request to participant
		// if any participant is not ready, return error to client and send abort request to all participants
		prepared := sendPrepareRequest(participant, transactionId, key, value)
		if !prepared {
		}
	}

	// Send out prepare request to all participants
	// after getting all prepare responses, if all participants are ready, send commit request to all participants, otherwise send abort request to all participants

}

func (twopc *TwoPhaseCommit) AbortTransaction(transactionId string) error {
}

func sendPrepareRequest(participant string, transactionId string, key string, value string) bool {

	return false

}

func sendAbortRequest(participant string, transactionId string) bool {
	return false
}

func sendCommitRequest(participant string, transactionId string) bool {

	return false
}
