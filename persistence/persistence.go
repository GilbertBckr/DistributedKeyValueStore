package persistence

import ()

type Transaction struct {
	Key   string
	Value string
	Id    string
}

type TransactionCoordinatorInfo struct {
	Id           string
	State        TransactionCoordinatorState
	Participants []ParticpantDB
}

type ParticpantDB struct {
	ID        string `json:"id"`
	ResultAck bool   `json:"state"`
}

type TransactionAndParticipants struct {
	Transaction  Transaction
	Participants []ParticpantDB
}
