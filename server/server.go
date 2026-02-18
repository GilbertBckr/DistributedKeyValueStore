package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"distributedKeyValue/persistence"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func StartServer(database persistence.DataPersistence) {

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.Write([]byte("Hello World!"))
	})

	r.Route("/crud", func(r chi.Router) {
		r.Get("/{key}", func(w http.ResponseWriter, r *http.Request) {
			key := chi.URLParam(r, "key")
			value, ok, err := database.Get(r.Context(), key)

			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			if !ok {
				// You requested 400, but note that 404 (Not Found) is standard here
				http.Error(w, "Key not found", http.StatusBadRequest)
				return
			}

			// Write the success response
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(value))
		})

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

			// 3. Call the database
			// database.Set signature: func(ctx, key, value) error
			err := database.Set(r.Context(), req.Key, req.Value)
			if err != nil {
				// Log the actual error internally here
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}

			// 4. Success Response
			w.WriteHeader(http.StatusCreated) // 201 Created
		})
	})

	fmt.Println("Starting Server")
	http.ListenAndServe(":3000", r)
	fmt.Println("Shutting down")
}
