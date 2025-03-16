package load

import (
	"atlassearch/model"
	dbutil "atlassearch/util"
	"github.com/google/uuid"
	"math/rand"
)

var creationStatsMap map[string]int

func loadDocuments() {
	// TODO: implement
	// steps
	// check number of documents in collection
	// if 1M then do not continue
	// else if less than 1M then delete
	// else
	// load creationStatsMap with document breakdown
	// each document should have its own id (UUID)
	// every 10 documents, change name of head chef
	// every 100 documents, change city
	// every 1000 documents, change state
	// every 10000 documents, change country
	// just use placeholder names
	// create documents and add to interface slice
	// insert all documents using a transaction
	util := dbutil.NewMongoDbUtil("")

	util.Close()
}

func loadIndices() {
	// TODO: implement
	// create the following indices:
	// restaurantId_1
	// firstName_1_lastName_1
	// city_1
	// state_1
	// country_1
}

func restaurantSkeleton() model.Restaurant {
	r := model.Restaurant{
		RestaurantName: "Restaurant " + randString(16),
		RestaurantId:   uuid.NewString(),
		MetaData:       model.Metadata{},
		Address:        model.Address{},
		Owners:         []model.Owner{},
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
