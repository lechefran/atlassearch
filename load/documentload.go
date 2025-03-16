package load

import (
	"atlassearch/model"
	util2 "atlassearch/util"
	"context"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"log"
	"math/rand"
)

func PrepareCollection() {
	util := util2.NewMongoDbUtil("")

	docCount := util.QueryMany(bson.D{})
	if len(docCount) == 1000000 {
		log.Println("Documents are already loaded in collection. Skipping document load step...")
	} else {
		log.Println("Loading documents to collection...")
		wc := writeconcern.Majority()
		txnOpts := options.Transaction().SetWriteConcern(wc)
		session, err := util.CreateSession()
		if err != nil {
			panic(err)
		}

		defer session.EndSession(context.Background())
		res, err := session.WithTransaction(context.TODO(), func(ctx context.Context) (interface{}, error) {
			docs := createDocuments()
			resInsert := util.InsertMany(docs)

			idIdx := mongo.IndexModel{ // restaurantId_1
				Keys: bson.D{{"restaurantId", 1}},
			}
			ownerIdx := mongo.IndexModel{ // firstName_text_lastName_text
				Keys: bson.D{{"owner.firstName", "text"},
					{"owner.lastName", "text"}},
			}
			cityIdx := mongo.IndexModel{ // city_text
				Keys: bson.D{{"address.city", "text"}},
			}
			stateIdx := mongo.IndexModel{ // state_text
				Keys: bson.D{{"address.state", "text"}},
			}
			countryIdx := mongo.IndexModel{ // country_text
				Keys: bson.D{{"address.country", "text"}},
			}

			resIdx := util.CreateIndex(idIdx) &&
				util.CreateIndex(ownerIdx) &&
				util.CreateIndex(cityIdx) &&
				util.CreateIndex(stateIdx) &&
				util.CreateIndex(countryIdx)
			return resInsert && resIdx, nil
		}, txnOpts)
		if res.(bool) {
			log.Println("All preparation steps have been executed successfully")
		} else {
			log.Println("Something went wrong in the preparation steps")
		}
	}
	util.Close()
}

func createDocuments() []model.Restaurant {
	var docs []model.Restaurant
	for i := 0; i < 1000000; i++ {
		docs = append(docs, restaurantSkeleton())
	}

	// create 10k document batches with the same country
	idx := 65
	country := "COUNTRY " + string(byte(idx))
	for i := 0; i < 1000000; i += 10000 {
		for j := 0; j < 10000; j++ {
			docs[i].Address.Country = country
		}
		idx++
	}

	// create 1k document batches with the same state
	idx = 65
	state := "STATE " + string(byte(idx))
	for i := 0; i < 1000000; i += 1000 {
		for j := 0; j < 1000; j++ {
			docs[i].Address.State = state
		}
		idx++
	}

	// create 100 document batches with the same city
	idx = 65
	city := "CITY " + string(byte(idx))
	for i := 0; i < 1000000; i += 100 {
		for j := 0; j < 100; j++ {
			docs[i].Address.City = city
		}
		idx++
	}

	// create 10 document batches with the same owner
	for i := 0; i < 1000000; i += 10 {
		owner := &model.Owner{
			OwnerId:   uuid.NewString(),
			FirstName: randString(16),
			LastName:  randString(16),
			Dob:       "00-00-0000",
		}
		for j := 0; j < 10; j++ {
			docs[i].Owner = *owner
		}
	}
	return docs
}

func DummyPreparation() { // dummy test function
	var docs []model.Restaurant
	for i := 0; i < 10; i++ {
		doc := restaurantSkeleton()
		doc.RestaurantName = randString(16)
		doc.MetaData.Type = randString(8)
		doc.MetaData.OperatingHours = []int{0, 24}
		doc.MetaData.PhoneNumber = randString(10)
		doc.MetaData.Email = randString(16)
		doc.MetaData.IsActive = true
		doc.Address.AddressId = uuid.NewString()
		doc.Address.City = randString(16)
		doc.Address.State = randString(16)
		doc.Address.Zip = randString(5)
		doc.Address.Country = randString(16)
		doc.Owner.OwnerId = uuid.NewString()
		doc.Owner.FirstName = randString(16)
		doc.Owner.LastName = randString(16)
		doc.Owner.Dob = randString(10)
		doc.Chefs = append(doc.Chefs, model.Chef{
			ChefId:     uuid.NewString(),
			FirstName:  randString(16),
			LastName:   randString(16),
			Dob:        randString(10),
			IsHeadChef: true,
		})
		doc.Menu = append(doc.Menu, model.MenuItem{
			Type:     randString(8),
			DishName: randString(16),
			Price: model.Price{
				Dollars: 0,
				Cents:   0,
			},
		})
		docs = append(docs, doc)
	}
}

func restaurantSkeleton() model.Restaurant {
	r := model.Restaurant{
		RestaurantName: "Restaurant " + randString(16),
		RestaurantId:   uuid.NewString(),
		MetaData:       model.Metadata{},
		Address:        model.Address{},
		Owner:          model.Owner{},
		Chefs:          []model.Chef{},
		Menu: []model.MenuItem{
			{
				Type:     "DISH",
				DishName: "DISH NUMBER 1",
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: "DISH NUMBER 2",
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: "DISH NUMBER 3",
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: "DISH NUMBER 4",
				Price:    model.Price{Dollars: 1, Cents: 99},
			},
			{
				Type:     "DISH",
				DishName: "DISH NUMBER 5",
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
