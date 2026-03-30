package twophasecommitcoordinator

import (
	"bytes"
	"context"
	"distributedKeyValue/persistence"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type collectVotesPersistenceManager interface {
	GetTransactionsInPhase1(context context.Context) ([]persistence.TransactionAndParticipants, error)
	SetTransactionCoordinatorAndOwnParticipantState(context context.Context, id string, state persistence.TransactionCoordinatorState) error
}

type serviceDiscoveryUrlGetter interface {
	GetUrlForParticipant(participantId string) string
}

var voteHttpClient = &http.Client{
	Timeout: 2 * time.Second,
}

func GetNewPhase1Runner(persistenceManager collectVotesPersistenceManager, sDiscovery serviceDiscoveryUrlGetter, channelManager *ChannelManager) func(context.Context) {

	return func(ctx context.Context) {

		for {
			// 1. Fetch open transactions
			transactions, err := persistenceManager.GetTransactionsInPhase1(ctx)
			if err != nil {
				slog.Error("failed to fetch transactions in phase 1", "error", err)
				// need to wait before retrying to avoid busy looping in case of persistent errors
				select {
				case <-ctx.Done():
					return // Exit immediately if shutting down
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}

			wg := sync.WaitGroup{}

			for _, transaction := range transactions {
				wg.Add(1)
				go func(transaction persistence.TransactionAndParticipants) {
					defer wg.Done()
					PerformPhase1ForTransaction(ctx, transaction, persistenceManager, sDiscovery, channelManager)
				}(transaction)
			}
			wg.Wait()

		}
	}
}

func PerformPhase1ForTransaction(context context.Context, transaction persistence.TransactionAndParticipants, persistenceManager collectVotesPersistenceManager, sDiscovery serviceDiscoveryUrlGetter, channelManager *ChannelManager) (persistence.TransactionCoordinatorState, error) {

	newState := checkVotesFromParticipantsParallel(context, transaction, sDiscovery)

	err := persistenceManager.SetTransactionCoordinatorAndOwnParticipantState(context, transaction.Transaction.Id, newState)

	if err != nil {
		slog.Error("failed to update transaction state after collecting votes", "transactionId", transaction.Transaction.Id, "error", err)
		return "", err
	}

	channelManager.NotifyHandler(transaction.Transaction.Id, newState)

	return newState, err

}

// Requests a vote from all participants and returns the new state of the transaction coordinator based on the votes, if any participant votes no, the transaction is aborted, otherwise it is committed
func checkVotesFromParticipantsParallel(ctx context.Context, transaction persistence.TransactionAndParticipants, sDiscovery serviceDiscoveryUrlGetter) persistence.TransactionCoordinatorState {

	numberParticipants := len(transaction.Participants)

	voteResults := make(chan bool, numberParticipants)

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()
	// Send requests in parallel
	for _, participant := range transaction.Participants {
		go func(p persistence.ParticpantDB) {
			couldCommit := requestVoteFromParticipant(ctx, p, transaction.Transaction, sDiscovery)
			voteResults <- couldCommit
		}(participant)
	}

	//Collect results
	for i := 0; i < numberParticipants; i++ {
		couldCommit := <-voteResults
		if !couldCommit {
			// Can safely return because the context cancellation will stop all other goroutines that are still waiting for a response from sending their result to the channel
			return persistence.TransactionCoordinatorStateAborted
		}
	}

	return persistence.TransactionCoordinatorStateCommitted

}

func requestVoteFromParticipant(ctx context.Context, participant persistence.ParticpantDB, transaction persistence.Transaction, sDiscovery serviceDiscoveryUrlGetter) bool {

	requestBody, err := json.Marshal(transaction)

	if err != nil {
		slog.Error("failed to marshal transaction for vote request", "transactionId", transaction.Id, "participantId", participant.ID, "error", err)
		return false
	}

	participantUrl := sDiscovery.GetUrlForParticipant(participant.ID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, participantUrl+"/transaction/vote", bytes.NewBuffer(requestBody))

	if err != nil {
		slog.Error("failed to create vote request for participant", "participantId", participant.ID, "error", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	respsonse, err := voteHttpClient.Do(req)

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
