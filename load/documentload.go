package load

import (
	dbutil "atlassearch/util"
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

func createRestaurantDocument() {
	// TODO: implement
}
