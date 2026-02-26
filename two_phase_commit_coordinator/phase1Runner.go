package twophasecommitcoordinator

import (
	"bytes"
	"context"
	"distributedKeyValue/persistence"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type collectVotesPersistenceManager interface {
	GetTransactionsInPhase1(context context.Context) ([]persistence.TransactionAndParticipants, error)
	SetTransactionCoordinatorAndOwnParticipantState(context context.Context, id string, state persistence.TransactionCoordinatorState) error
}

type serviceDiscoveryUrlGetter interface {
	GetUrlForParticipant(participantId string) string
}

func GetNewPhase1Runner(persistenceManager collectVotesPersistenceManager, sDiscovery serviceDiscoveryUrlGetter) func(context.Context) {

	return func(context context.Context) {

		for {
			// 1. Fetch open transactions
			transactions, err := persistenceManager.GetTransactionsInPhase1(context)
			if err != nil {
				slog.Error("failed to fetch transactions in phase 1", "error", err)
				continue
			}

			//TODO: we can optimize this by doing the vote request for all transactions in parallel, but for simplicity we do it sequentially here

			for _, transaction := range transactions {
				performPhase1ForTransaction(context, transaction, persistenceManager, sDiscovery)
			}

			time.Sleep(400 * time.Millisecond)
		}
	}
}

func performPhase1ForTransaction(context context.Context, transaction persistence.TransactionAndParticipants, persistenceManager collectVotesPersistenceManager, sDiscovery serviceDiscoveryUrlGetter) {

	newState := checkVotesFromParticipantsForTransaction(context, transaction, sDiscovery)

	err := persistenceManager.SetTransactionCoordinatorAndOwnParticipantState(context, transaction.Transaction.Id, newState)

	if err != nil {
		slog.Error("failed to update transaction state after phase 1", "transactionId", transaction.Transaction.Id, "error", err)
	}
}

// Requests a vote from all participants and returns the new state of the transaction coordinator based on the votes, if any participant votes no, the transaction is aborted, otherwise it is committed
func checkVotesFromParticipantsForTransaction(context context.Context, transaction persistence.TransactionAndParticipants, sDiscovery serviceDiscoveryUrlGetter) persistence.TransactionCoordinatorState {

	// TODO: refactor this to send requests in parallel later on

	for _, participant := range transaction.Participants {
		couldCommit := requestVoteFromParticipant(context, participant, transaction.Transaction, sDiscovery)
		if !couldCommit {
			return persistence.TransactionCoordinatorStateAborted
		}
	}
	return persistence.TransactionCoordinatorStateCommitted
}

func requestVoteFromParticipant(context context.Context, participant persistence.ParticpantDB, transaction persistence.Transaction, sDiscovery serviceDiscoveryUrlGetter) bool {

	requestBody, err := json.Marshal(transaction)

	if err != nil {
		panic(fmt.Errorf("failed to marshal transaction for vote request: %w", err))
	}

	participantUrl := sDiscovery.GetUrlForParticipant(participant.ID)

	req, err := http.NewRequestWithContext(context, http.MethodPut, participantUrl+"/transaction/vote", bytes.NewBuffer(requestBody))

	if err != nil {
		panic(fmt.Errorf("failed to create vote request for participant %s: %w", participant.ID, err))
	}

	//TODO: add timeout
	respsonse, err := http.DefaultClient.Do(req)

	if err != nil {
		slog.Warn("failed to send vote request to participant", "participantId", participant.ID, "error", err)
		return false
	}

	defer respsonse.Body.Close()

	switch respsonse.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusConflict:
		return false
	default:
		slog.Error("unexpected status code from participant", "participantId", participant.ID, "statusCode", respsonse.StatusCode)
		return false
	}

}
