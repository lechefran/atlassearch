package util

import (
	"atlassearch/model"
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"log"
)

type MongoDBUtil struct {
	client         *mongo.Client
	db, collection string
}

func NewMongoDbUtil(conn string) *MongoDBUtil {
	apiOpts := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(conn).SetServerAPIOptions(apiOpts)

	// create client
	client, err := mongo.Connect(opts)
	if err != nil {
		panic(err)
	}

	if err = client.Database("admin").RunCommand(context.TODO(),
		bson.D{{"ping", 1}}).Err(); err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected to MongoDB!")
	return &MongoDBUtil{
		client: client,
	}
}

func (u *MongoDBUtil) Database(d string) *MongoDBUtil {
	u.db = d
	return u
}

func (u *MongoDBUtil) Close() {
	if err := u.client.Disconnect(context.TODO()); err != nil {
		log.Println("Failed to close MongoDB connection gracefully")
		panic(err)
	}
}

func (u *MongoDBUtil) DeferClose() {
	defer func() {
		if err := u.client.Disconnect(context.TODO()); err != nil {
			log.Println("Failed to close MongoDB connection gracefully")
			panic(err)
		}
	}()
}

func (u *MongoDBUtil) Collection(c string) *MongoDBUtil {
	u.collection = c
	return u
}

func (u *MongoDBUtil) Query(d bson.D) []byte {
	var res []byte
	doc := bson.M{}
	if err := u.client.Database(u.db).Collection(u.collection).FindOne(context.TODO(), d).Decode(&doc); errors.Is(err, mongo.ErrNoDocuments) {
		fmt.Printf("No document was found in %s collection for given query", u.collection)
		return res
	} else if err != nil {
		panic(err)
	}

	res, err := bson.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return res
}

func (u *MongoDBUtil) Query2(m bson.M) []byte {
	var res []byte
	doc := bson.M{}
	if err := u.client.Database(u.db).Collection(u.collection).FindOne(context.TODO(), m).Decode(&doc); errors.Is(err, mongo.ErrNoDocuments) {
		fmt.Printf("No document was found in %s collection for given query", u.collection)
		return res
	} else if err != nil {
		panic(err)
	}

	res, err := bson.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return res
}

func (u *MongoDBUtil) Insert(r model.Restaurant) bool {
	if res, err := u.client.Database(u.db).Collection(u.collection).InsertOne(context.TODO(), r); err != nil {
		log.Println(err)
		return false
	} else if res.InsertedID != nil {
		return true
	}
	return true
}
