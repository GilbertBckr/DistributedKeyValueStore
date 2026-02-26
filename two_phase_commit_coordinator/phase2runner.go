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

type Phase2PersistenceManager interface {
	GetTransactionWaitingForAcknowledgement(context context.Context) ([]persistence.TransactionCoordinatorInfo, error)
	UpdateParticipantAckState(context context.Context, transactionId string, newParticipants []persistence.ParticpantDB, setDone bool) error
}
type AckRequest struct {
	TransactionId string                                  `json:"transactionId"`
	State         persistence.TransactionCoordinatorState `json:"state"`
}

func GetNewPhase2Runner(persistenceManager Phase2PersistenceManager, sDiscovery serviceDiscoveryUrlGetter) func(context.Context) {
	return func(context context.Context) {

		for {
			transactions, err := persistenceManager.GetTransactionWaitingForAcknowledgement(context)
			if err != nil {
				slog.Error("failed to fetch transaction waiting for acknowledgement", "error", err)
				time.Sleep(200 * time.Millisecond)
				continue
			}

			wg := sync.WaitGroup{}

			for _, transaction := range transactions {

				wg.Add(1)
				go func(t persistence.TransactionCoordinatorInfo) {
					defer wg.Done()
					performPhase2ForTransaction(context, t, persistenceManager, sDiscovery)
				}(transaction)
			}

			wg.Wait()

			time.Sleep(50 * time.Millisecond)
		}
	}
}

// This function performs phase 2 for a given transaction. It checks if we have received acknowledgements from all participants. If not, it sends requests to the participants to ask for the acknowledgements. Finally, it updates the participant ack state in the database and if we have received acknowledgements from all participants, it also sets the transaction as done.
func performPhase2ForTransaction(context context.Context, transaction persistence.TransactionCoordinatorInfo, persistenceManager Phase2PersistenceManager, sDiscovery serviceDiscoveryUrlGetter) {

	updatedParticipants, setDone := checkAcknowledgementFromParticipantsForTransaction(context, transaction, sDiscovery)

	err := persistenceManager.UpdateParticipantAckState(context, transaction.Id, updatedParticipants, setDone)

	if err != nil {
		slog.Error("failed to update participant ack state after phase 2", "transactionId", transaction.Id, "error", err)
	}
}

// This function iterates over the participants of the transaction and checks if we have received an acknowledgement from them. If not, it sends a request to the participant to ask for the acknowledgement. It returns an updated list of participants with their ack state and a boolean indicating if we have received acknowledgements from all participants.
func checkAcknowledgementFromParticipantsForTransaction(ctx context.Context, transaction persistence.TransactionCoordinatorInfo, sDiscovery serviceDiscoveryUrlGetter) ([]persistence.ParticpantDB, bool) {

	updatedParticipants := make([]persistence.ParticpantDB, len(transaction.Participants))

	wg := sync.WaitGroup{}

	for i, p := range transaction.Participants {
		wg.Add(1)
		go func(p persistence.ParticpantDB, i int) {
			defer wg.Done()

			if p.ResultAck {
				updatedParticipants[i] = p
				return
			}

			url := sDiscovery.GetUrlForParticipant(p.ID)

			ackReceived := sendAckRequestToParticipant(ctx, url, transaction.Id, transaction.State)

			updatedParticipants[i] = persistence.ParticpantDB{
				ID:        p.ID,
				ResultAck: ackReceived,
			}
		}(p, i)
	}
	wg.Wait()

	done := true
	// Looping over 3 participants is not a performance issue, so we can do this sequentially and it might be faster than an atomic bool idk
	for _, p := range updatedParticipants {
		if !p.ResultAck {
			done = false
			break // We found at least one missing ack, no need to check the rest
		}
	}

	return updatedParticipants, done
}

func sendAckRequestToParticipant(ctx context.Context, url string, transactionId string, state persistence.TransactionCoordinatorState) bool {

	bodyStruct := AckRequest{
		TransactionId: transactionId,
		State:         state,
	}

	body, err := json.Marshal(bodyStruct)
	if err != nil {
		slog.Error("failed to marshal body for ack request", "transactionId", transactionId, "error", err)
		return false
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, url+"/transaction/ack", bytes.NewBuffer(body))

	if err != nil {
		slog.Error("failed to create request for ack", "transactionId", transactionId, "error", err)
		return false
	}

	response, err := voteHttpClient.Do(request)

	if err != nil {
		slog.Error("failed to send ack request to participant", "transactionId", transactionId, "error", err)
		return false
	}

	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		return true
	} else {
		slog.Warn("received non ok response for ack request", "transactionId", transactionId, "statusCode", response.StatusCode)
		return false
	}

}
