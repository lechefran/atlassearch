package util

import (
	"atlassearch/model"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
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
	log.Println("Successfully connected to MongoDB!")
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

func (u *MongoDBUtil) Query(d bson.D, o ...model.SearchOptions) []byte {
	var res []byte
	doc := bson.D{}
	if err := u.client.Database(u.db).Collection(u.collection).FindOne(context.TODO(), d).Decode(&doc); errors.Is(err, mongo.ErrNoDocuments) {
		fmt.Printf("No document was found in %s collection for given query\n", u.collection)
		return res
	} else if err != nil {
		panic(err)
	}

	if len(o) > 0 && o[0].Explain {
		u.explain(d)
	}

	res, err := bson.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return res
}

func (u *MongoDBUtil) QueryMany(d bson.D, o ...model.SearchOptions) [][]byte {
	var res [][]byte
	cur, err := u.client.Database(u.db).Collection(u.collection).Find(context.TODO(), d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		fmt.Printf("No document was found in %s collection for given query\n", u.collection)
		return res
	} else if err != nil {
		panic(err)
	}

	if len(o) > 0 && o[0].Explain {
		u.explain(d)
	}

	for cur.Next(context.TODO()) {
		doc := bson.M{}
		if err := cur.Decode(&doc); err != nil {
			panic(err)
		} else {
			if tmp, err := bson.Marshal(doc); err == nil {
				res = append(res, tmp)
			}
		}
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

func (u *MongoDBUtil) InsertMany(d []model.Restaurant) bool {
	if _, err := u.client.Database(u.db).Collection(u.collection).InsertMany(context.TODO(), d); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (u *MongoDBUtil) Clear() bool {
	if res, err := u.client.Database(u.db).Collection(u.collection).DeleteMany(context.TODO(), bson.D{{}}); err != nil {
		log.Println(err)
		return false
	} else if res.DeletedCount == 0 {
		return true
	}
	return true
}

func (u *MongoDBUtil) ClearIndices() bool {
	if err := u.client.Database(u.db).Collection(u.collection).Indexes().DropAll(context.TODO()); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (u *MongoDBUtil) CreateIndex(m mongo.IndexModel) bool {
	if _, err := u.client.Database(u.db).Collection(u.collection).Indexes().CreateOne(context.TODO(), m); err != nil {
		log.Println(err)
		return false
	}
	return true
}

func (u *MongoDBUtil) CreateSession() (*mongo.Session, error) {
	return u.client.StartSession()
}

func (u *MongoDBUtil) Aggregate(p mongo.Pipeline) [][]byte {
	var res [][]byte
	cur, err := u.client.Database(u.db).Collection(u.collection).Aggregate(context.Background(), p)
	if err != nil {
		panic(err)
	}

	for cur.Next(context.TODO()) {
		doc := bson.M{}
		if err := cur.Decode(&doc); err != nil {
			panic(err)
		} else {
			if tmp, err := bson.Marshal(doc); err == nil {
				res = append(res, tmp)
			}
		}
	}
	return res
}

func (u *MongoDBUtil) explain(d bson.D) {
	cmd := bson.D{{"explain", bson.D{{"find", "search"}, {"filter", d}}}, {"verbosity", "allPlansExecution"}}
	opts := options.RunCmd().SetReadPreference(readpref.Primary())

	var result bson.M
	if err := u.client.Database(u.db).RunCommand(context.TODO(), cmd, opts).Decode(&result); err != nil {
		log.Fatal(err)
	}
	if formattedResult, err := json.MarshalIndent(result, "", "	"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(string(formattedResult))
	}
}
