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
	"net/url"
	"strconv"
)

const MongoConnString = ""
const MongoDatabase = ""
const MongoCollection = ""

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
	mux.HandleFunc("GET /scan/get-restaurant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)
		params := createSearchParams(r.URL.Query())

		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		restaurant := util.Query(*params, *opts)
		util.Close()

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
			},
			Response: []model.Restaurant{},
		}

		var doc model.Restaurant
		if err := bson.Unmarshal(restaurant, &doc); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			if doc.RestaurantId == "" {
				res.Status.Msg = "No restaurant was found!"
			} else {
				res.Status.Msg = "Found a restaurant!"
				res.Response = append(res.Response, doc)
			}
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /scan/get-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)
		params := createSearchParams(r.URL.Query())

		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		restaurants := util.QueryMany(*params, *opts)
		util.Close()

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
			},
			Response: []model.Restaurant{},
		}

		for _, r := range restaurants {
			var doc model.Restaurant
			if err := bson.Unmarshal(r, &doc); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				if doc.RestaurantId == "" {
					res.Status.Msg = "No restaurant was found!"
				} else {
					res.Status.Msg = "Found a restaurant!"
					res.Response = append(res.Response, doc)
				}
			}
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	mux.HandleFunc("GET /scan/get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		opts := createSearchOptions(r)

		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		restaurants := util.QueryMany(bson.D{{}}, *opts)
		util.Close()

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
			},
			Response: []model.Restaurant{},
		}

		for _, r := range restaurants {
			var doc model.Restaurant
			if err := bson.Unmarshal(r, &doc); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				if doc.RestaurantId == "" {
					res.Status.Msg = "No restaurant was found!"
				} else {
					res.Status.Msg = "Found a restaurant!"
					res.Response = append(res.Response, doc)
				}
			}
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	// atlas search handlers
	mux.HandleFunc("GET /atlas-search/get-restaurant", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		search := createAtlasSearchParams(r.URL.Query(), model.ParameterOptions{
			SearchIndex: r.URL.Query().Get("searchIndex"),
		})
		limit := bson.D{{"$limit", 1}}

		restaurants := util.Aggregate(mongo.Pipeline{*search, limit})
		util.Close()

		res := model.RestaurantResponse{
			Status: model.Status{
				Code: http.StatusOK,
			},
			Response: []model.Restaurant{},
		}

		for _, r := range restaurants {
			var doc model.Restaurant
			if err := bson.Unmarshal(r, &doc); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				if doc.RestaurantId == "" {
					res.Status.Msg = "No restaurant was found!"
				} else {
					res.Status.Msg = "Found a restaurant!"
					res.Response = append(res.Response, doc)
				}
			}
		}
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	//mux.HandleFunc("GET /atlas-search/get-restaurants", func(w http.ResponseWriter, r *http.Request) {
	//	w.Header().Set("Content-Type", "application/json")
	//	util := util2.NewMongoDbUtil(MongoConnString)
	//	search := bson.D{{"$search", createSearchParams(r, model.ParameterOptions{
	//		IsAtlasSearchQuery: true,
	//	})}}
	//	restaurants := util.Aggregate(mongo.Pipeline{search})
	//	util.Close()
	//
	//	msg := ""
	//	if len(restaurants) > 0 && len(restaurants[0]) > 0 {
	//		msg = "Found a restaurant!"
	//	} else {
	//		msg = "No restaurant was found!"
	//	}
	//
	//	res := model.RestaurantResponse{
	//		Status: model.Status{
	//			Code: http.StatusOK,
	//			Msg:  msg,
	//		},
	//		Response: restaurants,
	//	}
	//	if err := json.NewEncoder(w).Encode(res); err != nil {
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//})
	//mux.HandleFunc("GET /atlas-search/get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
	//	w.Header().Set("Content-Type", "application/json")
	//	util := util2.NewMongoDbUtil(MongoConnString)
	//	search := bson.D{{"$search", bson.D{}}}
	//	restaurants := util.Aggregate(mongo.Pipeline{search})
	//	util.Close()
	//
	//	msg := ""
	//	if len(restaurants) > 0 && len(restaurants[0]) > 0 {
	//		msg = "Found a restaurant!"
	//	} else {
	//		msg = "No restaurant was found!"
	//	}
	//
	//	res := model.RestaurantResponse{
	//		Status: model.Status{
	//			Code: http.StatusOK,
	//			Msg:  msg,
	//		},
	//		Response: restaurants,
	//	}
	//	if err := json.NewEncoder(w).Encode(res); err != nil {
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//})

	port := "8083"
	log.Println("Listening on port :" + port)
	err := http.ListenAndServe(":"+port, mux)
	log.Fatal(err)
}

func createSearchOptions(r *http.Request) *model.SearchOptions {
	res := model.SearchOptions{}
	if explain, err := strconv.ParseBool(r.URL.Query().Get("explain")); err != nil {
		res.Explain = false
	} else {
		res.Explain = explain
	}
	return &res
}

func createSearchParams(v url.Values) *bson.D {
	res := bson.D{}
	var arr []bson.M
	multi := len(v) > 0

	for k, v := range v {
		log.Printf("%s = %s\n", k, v[0])
		if k == "id" {
			createScanDocument(multi, "restaurantId", v[0], &res, &arr)
		} else if k == "firstName" {
			createScanDocument(multi, "owner.firstName", v[0], &res, &arr)
		} else if k == "lastName" {
			createScanDocument(multi, "owner.lastName", v[0], &res, &arr)
		} else if k == "city" {
			createScanDocument(multi, "address.city", v[0], &res, &arr)
		} else if k == "state" {
			createScanDocument(multi, "address.state", v[0], &res, &arr)
		} else if k == "country" {
			createScanDocument(multi, "address.country", v[0], &res, &arr)
		}
	}

	if multi {
		arr2 := bson.A{}
		for _, a := range arr {
			arr2 = append(arr2, a)
		}
		res = bson.D{{"$and", arr2}}
	}
	log.Println(res)
	return &res
}

func createAtlasSearchParams(v url.Values, o ...model.ParameterOptions) *bson.D {
	res := bson.D{}
	idx := ""
	if o[0].SearchIndex != "" {
		idx = o[0].SearchIndex
	} else {
		log.Println("No search index was provided. Atlas search query will use dynamic index")
		idx = "dynamic-search"
	}

	params := bson.D{{"index", idx}}
	for k, v := range v {
		log.Printf("%s = %s\n", k, v[0])
		if k == "id" {
			addNestedDoc(&params, "text", bson.D{{"path", "restaurantId"}, {"query", v[0]}})
		} else if k == "firstName" {
			addNestedDoc(&params, "text", bson.D{{"path", "owner.firstName"}, {"query", v[0]}})
		} else if k == "lastName" {
			addNestedDoc(&params, "text", bson.D{{"path", "owner.lastName"}, {"query", v[0]}})
		} else if k == "city" {
			addNestedDoc(&params, "text", bson.D{{"path", "address.city"}, {"query", v[0]}})
		} else if k == "state" {
			addNestedDoc(&params, "text", bson.D{{"path", "address.state"}, {"query", v[0]}})
		} else if k == "country" {
			addNestedDoc(&params, "text", bson.D{{"path", "address.country"}, {"query", v[0]}})
		}
	}
	addNestedDoc(&res, "$search", params)
	log.Println(res)
	return &res
}

func createScanDocument(b bool, k, v string, d *bson.D, m *[]bson.M) {
	if b {
		*m = append(*m, bson.M{k: bson.M{"$eq": v}})
	} else {
		*d = append(*d, bson.E{Key: k, Value: v})
	}
}

func addNestedDoc(doc *bson.D, key string, nested bson.D) {
	*doc = append(*doc, bson.E{Key: key, Value: nested})
}
