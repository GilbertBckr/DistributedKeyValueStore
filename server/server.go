package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	twophasecommit "distributedKeyValue/two_phase_commit"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func StartServer(transactionManager *twophasecommit.TwoPhaseCommit) {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.Write([]byte("Hello World!"))
	})

	r.Route("/crud", func(r chi.Router) {

		r.Post("/", func(w http.ResponseWriter, r *http.Request) {
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

			coudlCommit, id, err := transactionManager.StartNewTransaction(r.Context(), req.Key, req.Value)

			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to start transaction: %v", err), http.StatusInternalServerError)
				return
			}

			if !coudlCommit {
				http.Error(w, "Transaction could not be prepared due to a conflict", http.StatusConflict)
				return
			}

			w.Header().Add("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{
				"transactionId": id,
				"message":       "Transaction prepared successfully, commit phase will be handled by the scheduler",
			})
		})
	})

	fmt.Println("Starting Server")
	http.ListenAndServe(":3000", r)
	fmt.Println("Shutting down")
}
