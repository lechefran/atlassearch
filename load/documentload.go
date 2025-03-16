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
	// else if less than 1M then clear collection
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

	var docs []model.Restaurant

	// create 10k document batches with the same country
	idx := 65
	country := "COUNTRY " + string(byte(idx))
	for i := 0; i < 1000000; i += 10000 {
		for j := 0; j < 10000; j++ {
			doc := restaurantSkeleton()
			doc.Address.Country = country
			docs = append(docs, doc)
		}
		idx++
	}

	// create 1k document batches with the same state
	idx = 65
	state := "STATE " + string(byte(idx))
	for i := 0; i < 1000000; i += 1000 {
		for j := 0; j < 1000; j++ {
			doc := restaurantSkeleton()
			doc.Address.State = state
			docs = append(docs, doc)
		}
		idx++
	}

	// create 100 document batches with the same city
	idx = 65
	city := "CITY " + string(byte(idx))
	for i := 0; i < 1000000; i += 100 {
		for j := 0; j < 100; j++ {
			doc := restaurantSkeleton()
			doc.Address.City = city
			docs = append(docs, doc)
		}
		idx++
	}

	// create 10 document batches with the same chef name
	for i := 0; i < 1000000; i += 10 {
		headChef := &model.Chef{
			ChefId:     uuid.NewString(),
			FirstName:  randString(16),
			LastName:   randString(16),
			Dob:        "00-00-0000",
			IsHeadChef: true,
		}
		chef := &model.Chef{
			ChefId:     uuid.NewString(),
			FirstName:  randString(16),
			LastName:   randString(16),
			Dob:        "00-00-0000",
			IsHeadChef: false,
		}
		for j := 0; j < 10; j++ {
			doc := restaurantSkeleton()
			doc.Chefs = append(doc.Chefs, *headChef)
			doc.Chefs = append(doc.Chefs, *chef)
			docs = append(docs, doc)
		}
	}

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
