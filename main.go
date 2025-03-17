package main

import (
	"atlassearch/load"
	"atlassearch/model"
	util2 "atlassearch/util"
	"encoding/json"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"log"
	"net/http"
	"strconv"
	"strings"
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
		req := model.InstallRequest{}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else {
			if req.Install == "" {
				http.Error(w, "Installation parameter required: full or dummy", http.StatusBadRequest)
			} else if req.Install == "dummy" {
				log.Println("Starting dummy installation...")
				load.DummyPreparation()
			} else {
				log.Println("Starting full installation...")
				load.PrepareCollection()
			}
		}
		res := model.StatusResponse{
			Code:  http.StatusAccepted,
			Title: http.StatusText(http.StatusAccepted),
			Msg:   "Starting installation...",
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// column and index scan handlers
	// TODO: remove
	mux.HandleFunc("GET /scan/get-restaurant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)
		params := createSearchParams(r)

		util := util2.NewMongoDbUtil("")
		restaurant := util.Query(*params, *opts)
		util.Close()

		msg := ""
		if len(restaurant) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: make([][]byte, 0),
		}
		res.Response = append(res.Response, restaurant)
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /scan/get-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)
		params := createSearchParams(r)

		util := util2.NewMongoDbUtil("")
		restaurants := util.QueryMany(*params, *opts)
		util.Close()

		msg := ""
		if len(restaurants) > 0 && len(restaurants[0]) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: restaurants,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /scan/get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)

		util := util2.NewMongoDbUtil("")
		restaurants := util.QueryMany(bson.D{{}}, *opts)
		util.Close()

		msg := ""
		if len(restaurants) > 0 && len(restaurants[0]) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: restaurants,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// atlas search handlers
	// TODO: remove
	mux.HandleFunc("GET /atlas-search/get-restaurant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil("")
		search := bson.D{} // TODO: implement
		limit := bson.D{{"$limit", 1}}
		restaurants := util.Aggregate(mongo.Pipeline{search, limit})
		util.Close()

		msg := ""
		if len(restaurants) > 0 && len(restaurants[0]) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: restaurants,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /atlas-search/get-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil("")
		search := bson.D{} // TODO: implement
		restaurants := util.Aggregate(mongo.Pipeline{search})
		util.Close()

		msg := ""
		if len(restaurants) > 0 && len(restaurants[0]) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: restaurants,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /atlas-search/get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil("")
		search := bson.D{{"$search", bson.D{}}}
		restaurants := util.Aggregate(mongo.Pipeline{search})
		util.Close()

		msg := ""
		if len(restaurants) > 0 && len(restaurants[0]) > 0 {
			msg = "Found a restaurant!"
		} else {
			msg = "No restaurant was found!"
		}

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
				Msg:  msg,
			},
			Response: restaurants,
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func createSearchOptions(r *http.Request) *model.SearchOptions {
	res := model.SearchOptions{}
	if explain, err := strconv.ParseBool(r.URL.Query().Get("explain")); err != nil {
		res.Explain = false
	} else {
		res.Explain = explain
	}
	if scan := r.URL.Query().Get("scanType"); scan == "" {
		res.ScanType = "column"
	} else {
		res.ScanType = strings.ToLower(scan)
	}
	return &res
}

func createSearchParams(r *http.Request) *bson.D {
	res := bson.D{}
	if resId := r.URL.Query().Get("id"); resId != "" {
		res = append(res, bson.E{Key: "restaurantId", Value: resId})
	}
	if firstName := r.URL.Query().Get("firstName"); firstName != "" {
		res = append(res, bson.E{Key: "owner.firstName", Value: firstName})
	}
	if lastName := r.URL.Query().Get("lastName"); lastName != "" {
		res = append(res, bson.E{Key: "owner.lastName", Value: lastName})
	}
	if city := r.URL.Query().Get("city"); city != "" {
		res = append(res, bson.E{Key: "owner.city", Value: city})
	}
	if state := r.URL.Query().Get("state"); state != "" {
		res = append(res, bson.E{Key: "owner.state", Value: state})
	}
	if country := r.URL.Query().Get("country"); country != "" {
		res = append(res, bson.E{Key: "owner.country", Value: country})
	}
	return &res
}
