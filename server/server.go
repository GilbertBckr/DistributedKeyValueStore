package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"distributedKeyValue/persistence"
	twophasecommitcoordinator "distributedKeyValue/two_phase_commit_coordinator"
	twophasecommitparticipant "distributedKeyValue/two_phase_commit_participant"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func StartServer(transactionManager *twophasecommitcoordinator.TwoPhaseCommit, transactionParticipant *twophasecommitparticipant.TwoPhaseCommitParticipant, channelManager *twophasecommitcoordinator.ChannelManager) {
	r := chi.NewRouter()
	r.Use(ResponseLoggerMiddleware)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.Write([]byte("Hello World!"))
	})

	r.Route("/transaction", func(r chi.Router) {
		r.Put("/vote", func(w http.ResponseWriter, r *http.Request) {
			adapterRequestVote(w, r, transactionParticipant)
		})
		r.Put("/ack", func(w http.ResponseWriter, r *http.Request) {
			adapterAckTransactionResult(w, r, transactionParticipant)
		})
		r.Get("/status/{transactionId}", func(w http.ResponseWriter, r *http.Request) {
			transactionId := chi.URLParam(r, "transactionId")
			status, err := transactionParticipant.GetTransactionStatus(r.Context(), transactionId)

			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to get transaction status: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Add("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"transactionId": transactionId,
				"status":        string(status),
			})
		})
	})

	r.Route("/crud", func(r chi.Router) {
		r.Post("/", func(w http.ResponseWriter, r *http.Request) {
			adaterPostKey(w, r, transactionManager, channelManager)
		})
		r.Get("/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := chi.URLParam(r, "key")
			value, err := twophasecommitparticipant.HandleGetRequest(r.Context(), key, transactionParticipant.PersistenceManager)

			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to get value for key: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Add("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"key":   key,
				"value": value,
			})
		})
	})

	fmt.Println("Starting Server")
	http.ListenAndServe(":3000", r)
	fmt.Println("Shutting down")
}

func adapterAckTransactionResult(w http.ResponseWriter, r *http.Request, transactionParticipant *twophasecommitparticipant.TwoPhaseCommitParticipant) {

	var request twophasecommitcoordinator.AckRequest

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4000)).Decode(&request); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}
	if request.TransactionId == "" {
		http.Error(w, "Transaction ID cannot be empty", http.StatusBadRequest)
		return
	}

	err := transactionParticipant.HandleAckRequest(r.Context(), request.TransactionId, request.State)

	if err != nil {
		slog.Error("Failed to handle ack request", "error", err)
		http.Error(w, fmt.Sprintf("Failed to handle ack request: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Final Transaction State received and processed"))

}

func adaterPostKey(w http.ResponseWriter, r *http.Request, transactionManager *twophasecommitcoordinator.TwoPhaseCommit, channelManger *twophasecommitcoordinator.ChannelManager) {
	var req SetRequest

	// 1. Decode the JSON body
	// We limit the request body size to 1MB to prevent DOS attacks
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1048576)).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// 2. Validate input
	if req.Key == "" {
		http.Error(w, "Key cannot be empty", http.StatusBadRequest)
		return
	}

	tid := twophasecommitcoordinator.GetUniqueTransactionId()

	channel := make(chan persistence.TransactionCoordinatorState, 1)

	channelManger.Add(tid, channel)

	defer channelManger.Remove(tid)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)

	defer cancel()

	couldCommit, id, err := transactionManager.StartNewTransaction(r.Context(), tid, req.Key, req.Value)

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to start transaction: %v", err), http.StatusInternalServerError)
		return
	}

	if !couldCommit {
		http.Error(w, "Transaction could not be prepared due to a conflict", http.StatusConflict)
		return
	}

	select {
	case result := <-channel:
		slog.Info("Received transaction result", "transactionId", id, "result", result)
		if result == persistence.TransactionCoordinatorStateCommitted {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Transaction committed successfully"))
		} else if result == persistence.TransactionCoordinatorStateAborted {
			http.Error(w, "Transaction was aborted", http.StatusBadRequest)
		} else {
			panic(fmt.Sprintf("Received unknown transaction coordinator state: %s", result))
		}

	case <-ctx.Done():
		w.Header().Add("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"transactionId": id,
			"message":       "Transaction prepared successfully, commit phase will be handled by the scheduler",
		})
	}
}

func adapterRequestVote(w http.ResponseWriter, r *http.Request, transactionParticipant *twophasecommitparticipant.TwoPhaseCommitParticipant) {

	var transaction persistence.Transaction

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4000)).Decode(&transaction); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	couldPrepare, err := transactionParticipant.HandlePrepareRequest(r.Context(), transaction)

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to handle prepare request: %v", err), http.StatusInternalServerError)
		return
	}

	if couldPrepare {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Prepared"))
	} else {
		http.Error(w, "Could not prepare transaction due to a conflict", http.StatusConflict)
	}

}

func ResponseLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap the original ResponseWriter
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		// Create a buffer to store a copy of the response body
		var buf bytes.Buffer
		ww.Tee(&buf)

		// Process the request
		next.ServeHTTP(ww, r)

		// Log the captured response along with request details
		slog.Info("HTTP Traffic",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"response_bytes", ww.BytesWritten(),
			"response_body", buf.String(),
		)
	})
}
