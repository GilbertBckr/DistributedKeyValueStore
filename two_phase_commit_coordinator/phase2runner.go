package twophasecommitcoordinator

import (
	"bytes"
	"context"
	"distributedKeyValue/persistence"
	"encoding/json"
	"log/slog"
	"net/http"
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
				continue
			}

			for _, transaction := range transactions {
				performPhase2ForTransaction(context, transaction, persistenceManager, sDiscovery)
			}

			time.Sleep(400 * time.Millisecond)
		}
	}
}

func performPhase2ForTransaction(context context.Context, transaction persistence.TransactionCoordinatorInfo, persistenceManager Phase2PersistenceManager, sDiscovery serviceDiscoveryUrlGetter) {

	updatedParticipants, setDone := checkAcknowledgementFromParticipantsForTransaction(context, transaction, sDiscovery)

	err := persistenceManager.UpdateParticipantAckState(context, transaction.Id, updatedParticipants, setDone)

	if err != nil {
		slog.Error("failed to update participant ack state after phase 2", "transactionId", transaction.Id, "error", err)
	}
}

func checkAcknowledgementFromParticipantsForTransaction(context context.Context, transaction persistence.TransactionCoordinatorInfo, sDiscovery serviceDiscoveryUrlGetter) ([]persistence.ParticpantDB, bool) {

	updatedParticipants := make([]persistence.ParticpantDB, len(transaction.Participants))

	done := true
	for i, participant := range transaction.Participants {
		if participant.ResultAck {
			updatedParticipants[i] = participant
			continue
		}

		url := sDiscovery.GetUrlForParticipant(participant.ID)

		ackReceived := sendAckRequestToParticipant(context, url, transaction.Id, transaction.State)

		updatedParticipants[i] = persistence.ParticpantDB{
			ID:        participant.ID,
			ResultAck: ackReceived,
		}
		if !ackReceived {
			done = false
		}
	}

	return updatedParticipants, done
}

func sendAckRequestToParticipant(context context.Context, url string, transactionId string, state persistence.TransactionCoordinatorState) bool {

	bodyStruct := AckRequest{
		TransactionId: transactionId,
		State:         state,
	}

	body, err := json.Marshal(bodyStruct)
	if err != nil {
		slog.Error("failed to marshal body for ack request", "transactionId", transactionId, "error", err)
		return false
	}

	request, err := http.NewRequestWithContext(context, http.MethodPut, url+"/transaction/ack", bytes.NewBuffer(body))

	if err != nil {
		slog.Error("failed to create request for ack", "transactionId", transactionId, "error", err)
		return false
	}

	response, err := http.DefaultClient.Do(request)

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
