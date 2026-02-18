package persistence

import (
	"context"
	"database/sql"
	servicediscovery "distributedKeyValue/service_discovery"
)

type Transaction struct {
	Key   string
	Value string
	Id    string
}

type ParticpantDB struct {
	servicediscovery.Participant
	State TransactionState `json:"state"`
}

type TransactionManagerPersistence interface {
	//Get(context context.Context, key string) (string, bool, error)
	// Inserts the transaction into the database with state "prepared". If there is a conflict with an existing transaction, it should return false. If the transaction is successfully prepared, it should return true.
	TryPrepareTransaction(context context.Context, transaction Transaction, optTx *sql.Tx) (bool, error)
	AbortTransaction(context context.Context, id string) error
	//CommitTransaction(context context.Context, id string) error
	//GetTransactionState(context context.Context, id string) (string, bool, error)
	TransactionCoordinatorStartTransaction(context context.Context, transaction Transaction, participants []ParticpantDB) (bool, error)
}
