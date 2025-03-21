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
	"os"
	"strconv"
)

func main() {
	MongoConnString := os.Getenv("MONGO_DB_CONN_STRING")
	MongoDatabase := os.Getenv("MONGO_DB_DATABASE")
	MongoCollection := os.Getenv("MONGO_DB_COLLECTION")

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
			loadIndexes := false
			s := r.URL.Query().Get("loadIndexes")
			if s != "" {
				loadIndexes, _ = strconv.ParseBool(s)
			}

			docCount := req.DocumentCount
			if docCount == 0 {
				docCount = 1000000
			} else if docCount%10000 != 0 {
				http.Error(w, "Document count can only be in batches of 10000", http.StatusBadRequest)
				return
			}

			if req.Install == "" {
				http.Error(w, "Installation parameter required: full or dummy", http.StatusBadRequest)
			} else if req.Install == "dummy" {
				log.Println("Starting dummy installation...")
				load.PrepareDummyCollection(loadIndexes)
			} else {
				log.Println("Starting full installation...")
				go load.PrepareCollection(loadIndexes, docCount) // goroutine
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

	port := os.Getenv("PORT")
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

	for k, v := range v {
		log.Printf("%s = %s\n", k, v[0])
		if k == "id" {
			arr = append(arr, bson.M{"restaurantId": bson.M{"$eq": v}})
		} else if k == "firstName" {
			arr = append(arr, bson.M{"owner.firstName": bson.M{"$eq": v}})
		} else if k == "lastName" {
			arr = append(arr, bson.M{"owner.lastName": bson.M{"$eq": v}})
		} else if k == "city" {
			arr = append(arr, bson.M{"address.city": bson.M{"$eq": v}})
		} else if k == "state" {
			arr = append(arr, bson.M{"address.state": bson.M{"$eq": v}})
		} else if k == "country" {
			arr = append(arr, bson.M{"address.country": bson.M{"$eq": v}})
		}
	}

	arr2 := bson.A{}
	for _, a := range arr {
		arr2 = append(arr2, a)
	}
	res = bson.D{{"$and", arr2}}
	log.Printf("Scan query: %s", res)
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
	for k, v := range v {
		log.Printf("%s = %s\n", k, v[0])
		if k == "id" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "restaurantId"}, {"query", v[0]}}}})
		} else if k == "firstName" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "owner.firstName"}, {"query", v[0]}}}})
		} else if k == "lastName" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "owner.lastName"}, {"query", v[0]}}}})
		} else if k == "city" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "address.city"}, {"query", v[0]}}}})
		} else if k == "state" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "address.state"}, {"query", v[0]}}}})
		} else if k == "country" {
			must = append(must, bson.D{{Key: "text", Value: bson.D{{"path", "address.country"}, {"query", v[0]}}}})
		}
	}

	addNestedDoc(&params, "compound", bson.D{{"must", must}})
	addNestedDoc(&res, "$search", params)
	log.Printf("Atlas search query: %s", res)
	return &res
}

func addNestedDoc(doc *bson.D, key string, nested bson.D) {
	*doc = append(*doc, bson.E{Key: key, Value: nested})
}
