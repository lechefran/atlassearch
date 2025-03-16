package main

import (
	"atlassearch/load"
	"atlassearch/model"
	"encoding/json"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) { // health check
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(model.StatusResponse{
			Code:  http.StatusOK,
			Title: http.StatusText(http.StatusOK),
			Msg:   "pong!",
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("POST /run-install", func(w http.ResponseWriter, r *http.Request) {
		res := model.StatusResponse{}
		req := model.InstallRequest{}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else {
			if req.Install == "" {
				http.Error(w, "Install required: full or dummy", http.StatusBadRequest)
			} else if req.Install == "dummy" {
				log.Println("Starting dummy installation...")
				load.DummyPreparation()
			} else {
				log.Println("Starting full installation...")
				load.PrepareCollection()
			}
		}
		res = model.StatusResponse{
			Code:  http.StatusAccepted,
			Title: http.StatusText(http.StatusAccepted),
			Msg:   "Starting installation...",
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// column scan handlers
	mux.HandleFunc("GET /get-restaurant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	})
	mux.HandleFunc("GET /get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	})

	// index scan handlers
	mux.HandleFunc("GET /get-restaurant-as", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	})
	mux.HandleFunc("GET /get-all-restaurants-as", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
	})
}
