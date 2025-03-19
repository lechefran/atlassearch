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
	mux.HandleFunc("GET /atlas-search/get-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		search := createAtlasSearchParams(r.URL.Query(), model.ParameterOptions{
			SearchIndex: r.URL.Query().Get("searchIndex"),
		})

		restaurants := util.Aggregate(mongo.Pipeline{*search})
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
	mux.HandleFunc("GET /atlas-search/get-all-restaurants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		util := util2.NewMongoDbUtil(MongoConnString).Database(MongoDatabase).Collection(MongoCollection)
		search := bson.D{}

		params := bson.D{{"index", "dynamic-search"}}
		must := bson.A{}
		must = append(must, bson.D{{Key: "exists", Value: bson.D{{"path", "_id"}}}})
		addNestedDoc(&params, "compound", bson.D{{"must", must}})
		addNestedDoc(&search, "$search", params)
		log.Println(params)

		restaurants := util.Aggregate(mongo.Pipeline{search})
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
			createScanDoc(multi, "restaurantId", v[0], &res, &arr)
		} else if k == "firstName" {
			createScanDoc(multi, "owner.firstName", v[0], &res, &arr)
		} else if k == "lastName" {
			createScanDoc(multi, "owner.lastName", v[0], &res, &arr)
		} else if k == "city" {
			createScanDoc(multi, "address.city", v[0], &res, &arr)
		} else if k == "state" {
			createScanDoc(multi, "address.state", v[0], &res, &arr)
		} else if k == "country" {
			createScanDoc(multi, "address.country", v[0], &res, &arr)
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
	must := bson.A{}
	multi := len(v) > 0
	for k, v := range v {
		log.Printf("%s = %s\n", k, v[0])
		if k == "id" {
			createAtlasSearchDoc(multi, "restaurantId", v[0], &params, &must)
		} else if k == "firstName" {
			createAtlasSearchDoc(multi, "owner.firstName", v[0], &params, &must)
		} else if k == "lastName" {
			createAtlasSearchDoc(multi, "owner.lastName", v[0], &params, &must)
		} else if k == "city" {
			createAtlasSearchDoc(multi, "address.city", v[0], &params, &must)
		} else if k == "state" {
			createAtlasSearchDoc(multi, "address.state", v[0], &params, &must)
		} else if k == "country" {
			createAtlasSearchDoc(multi, "address.country", v[0], &params, &must)
		}
	}
	if multi {
		addNestedDoc(&params, "compound", bson.D{{"must", must}})
	}
	addNestedDoc(&res, "$search", params)
	log.Println(res)
	return &res
}

func createScanDoc(b bool, k, v string, d *bson.D, m *[]bson.M) {
	if b {
		*m = append(*m, bson.M{k: bson.M{"$eq": v}})
	} else {
		*d = append(*d, bson.E{Key: k, Value: v})
	}
}

func createAtlasSearchDoc(b bool, k, v string, d *bson.D, a *bson.A) {
	if b {
		*a = append(*a, bson.D{{Key: "text", Value: bson.D{{"path", k}, {"query", v}}}})
	} else {
		addNestedDoc(d, "text", bson.D{{"path", k}, {"query", v}})
	}
}

func addNestedDoc(doc *bson.D, key string, nested bson.D) {
	*doc = append(*doc, bson.E{Key: key, Value: nested})
}
