package load

import (
	"atlassearch/model"
	util2 "atlassearch/util"
	"context"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"log"
	"math/rand"
	"os"
	"time"
)

func PrepareCollection(loadIndexes bool, docCount int64) {
	util := util2.NewMongoDbUtil(os.Getenv("MONGO_DB_CONN_STRING")).Database(os.Getenv("MONGO_DB_DATABASE")).Collection(os.Getenv("MONGO_DB_COLLECTION"))
	log.Println("Loading documents to collection...")
	wc := writeconcern.Majority()
	txnOpts := options.Transaction().SetWriteConcern(wc)
	session, err := util.CreateSession()
	if err != nil {
		panic(err)
	}

	defer session.EndSession(context.Background())
	res, err := session.WithTransaction(context.TODO(), func(ctx context.Context) (interface{}, error) {
		var resInsert bool
		if len(util.QueryMany(bson.D{})) > 0 {
			log.Println("Documents are present in collection. Skipping document load ...")
			resInsert = true
		} else {
			log.Println("Starting document creation and insertion...")
			util.Clear()

			times := docCount / 10000
			for range times {
				go func() {
					docs := createDocuments()
					resInsert = util.InsertMany(docs)
				}()
				time.Sleep(time.Second)
			}
		}

		var resIdx bool
		if !loadIndexes {
			log.Println("No indexes will be loaded. Skipping index load ...")
			resIdx = true
		} else {
			log.Println("Starting index initialization...")
			util.ClearIndices()
			idIdx := mongo.IndexModel{ // restaurantId_1
				Keys:    bson.D{{"restaurantId", 1}},
				Options: options.Index().SetUnique(true),
			}
			ownerIdx := mongo.IndexModel{ // firstName_text_lastName_text
				Keys: bson.D{{"owner.firstName", 1},
					{"owner.lastName", 1}},
			}
			cityIdx := mongo.IndexModel{ // city_text
				Keys: bson.D{{"address.city", 1}},
			}
			stateIdx := mongo.IndexModel{ // state_text
				Keys: bson.D{{"address.state", 1}},
			}
			countryIdx := mongo.IndexModel{ // country_text
				Keys: bson.D{{"address.country", 1}},
			}

			log.Println("Starting index creation...")
			resIdx = util.CreateIndex(idIdx) &&
				util.CreateIndex(ownerIdx) &&
				util.CreateIndex(cityIdx) &&
				util.CreateIndex(stateIdx) &&
				util.CreateIndex(countryIdx)
		}
		return resInsert && resIdx, nil
	}, txnOpts)
	if res.(bool) {
		log.Println("All preparation steps have been executed successfully")
	} else {
		log.Println("Something went wrong in the preparation steps")
	}
	util.Close()
}

func PrepareDummyCollection(loadIndexes bool) { // dummy test function
	util := util2.NewMongoDbUtil(os.Getenv("MONGO_DB_CONN_STRING")).Database(os.Getenv("MONGO_DB_DATABASE")).Collection(os.Getenv("MONGO_DB_COLLECTION"))
	log.Println("Loading documents to collection...")
	wc := writeconcern.Majority()
	txnOpts := options.Transaction().SetWriteConcern(wc)
	session, err := util.CreateSession()
	if err != nil {
		panic(err)
	}

	defer session.EndSession(context.Background())
	res, err := session.WithTransaction(context.TODO(), func(ctx context.Context) (interface{}, error) {
		var resInsert bool
		log.Println("Starting document creation...")
		util.Clear()
		docs := createDocuments()

		log.Println("Starting document insertion...")
		resInsert = util.InsertMany(docs)

		var resIdx bool
		if !loadIndexes {
			log.Println("No indexes will be loaded. Skipping index load ...")
			resIdx = true
		} else {
			log.Println("Starting index initialization...")
			util.ClearIndices()
			idIdx := mongo.IndexModel{ // restaurantId_1
				Keys:    bson.D{{"restaurantId", 1}},
				Options: options.Index().SetUnique(true),
			}
			ownerIdx := mongo.IndexModel{ // firstName_text_lastName_text
				Keys: bson.D{{"owner.firstName", 1},
					{"owner.lastName", 1}},
			}
			cityIdx := mongo.IndexModel{ // city_text
				Keys: bson.D{{"address.city", 1}},
			}
			stateIdx := mongo.IndexModel{ // state_text
				Keys: bson.D{{"address.state", 1}},
			}
			countryIdx := mongo.IndexModel{ // country_text
				Keys: bson.D{{"address.country", 1}},
			}

			log.Println("Starting index creation...")
			resIdx = util.CreateIndex(idIdx) &&
				util.CreateIndex(ownerIdx) &&
				util.CreateIndex(cityIdx) &&
				util.CreateIndex(stateIdx) &&
				util.CreateIndex(countryIdx)
		}
		return resInsert && resIdx, nil
	}, txnOpts)
	if res.(bool) {
		log.Println("All preparation steps have been executed successfully")
	} else {
		log.Println("Something went wrong in the preparation steps")
	}
	util.Close()
}

// create a batch of 10000 documents
func createDocuments() []model.Restaurant {
	var docs []model.Restaurant
	for i := 0; i < 10000; i++ {
		docs = append(docs, restaurantSkeleton())
	}

	// create 10k document batches with the same country
	country := randString(24)
	for i := 0; i < 10000; i++ {
		docs[i].Address.AddressId = randString(32)
		docs[i].Address.Zip = randString(5)
		docs[i].Address.Country = country
	}

	// create 1k document batches with the same state
	for i := 0; i < 10000; i += 1000 {
		state := randString(24)
		for j := i; j < i+1000; j++ {
			docs[j].Address.State = state
		}
	}

	// create 100 document batches with the same city
	for i := 0; i < 10000; i += 100 {
		city := randString(24)
		for j := i; j < i+100; j++ {
			docs[j].Address.City = city
		}
	}

	// create 10 document batches with the same owner
	for i := 0; i < 10000; i += 10 {
		id := randString(24)
		firstName := randString(16)
		lastName := randString(16)
		dob := "00-00-0000"

		for j := i; j < i+10; j++ {
			docs[j].Owner.OwnerId = id
			docs[j].Owner.FirstName = firstName
			docs[j].Owner.LastName = lastName
			docs[j].Owner.Dob = dob
		}
	}
	return docs
}

func restaurantSkeleton() model.Restaurant {
	r := model.Restaurant{
		RestaurantName: randString(16),
		RestaurantId:   randString(48),
		MetaData: model.Metadata{
			Type: "restaurant",
		},
		Address: model.Address{},
		Owner:   model.Owner{},
		Chefs: []model.Chef{
			{
				ChefId:     randString(16),
				FirstName:  randString(16),
				LastName:   randString(16),
				Dob:        "00-00-0000",
				IsHeadChef: true,
			},
			{
				ChefId:     randString(16),
				FirstName:  randString(16),
				LastName:   randString(16),
				Dob:        "00-00-0000",
				IsHeadChef: false,
			},
		},
		Menu: []model.MenuItem{
			{
				Type:     "DISH",
				DishName: randString(30),
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: randString(30),
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: randString(30),
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: randString(30),
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: randString(30),
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
		},
	}
	return r
}

func randString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = byte(rand.Intn(26) + 65)
	}
	return string(b)
}
