package persistence

import "context"

type Transaction struct {
	key   string
	value string
	id    string
}

type TransactionManagerPersistence interface {
	Get(context context.Context, key string) (string, bool, error)
	// Inserts the transaction into the database with state "prepared". If there is a conflict with an existing transaction, it should return false. If the transaction is successfully prepared, it should return true.
	TryPerpareTransaction(context context.Context, transaction Transaction) bool
	AbortTransaction(context context.Context, id string) error
	CommitTransaction(context context.Context, id string) error
	GetTransactionState(context context.Context, id string) (string, bool, error)
}
