package model

type Restaurant struct {
	RestaurantName string     `json:"restaurantName" bson:"restaurantName"`
	RestaurantId   string     `json:"restaurantId" bson:"restaurantId"`
	MetaData       Metadata   `json:"metaData" bson:"metaData"`
	Address        Address    `json:"address" bson:"address"`
	Owners         []Owner    `json:"owners" bson:"owners"`
	Chefs          []Chef     `json:"chefs" bson:"chefs"`
	Menu           []MenuItem `json:"menu" bson:"menu"`
}

type Metadata struct {
	Type           string `json:"type" bson:"type"`
	OperatingHours []int  `json:"operatingHours" bson:"operatingHours"`
	PhoneNumber    string `json:"phoneNumber" bson:"phoneNumber"`
	Email          string `json:"email" bson:"email"`
	IsActive       bool   `json:"isActive" bson:"isActive"`
}

type Address struct {
	AddressId string `json:"addressId" bson:"addressId"`
	City      string `json:"city" bson:"city"`
	State     string `json:"state" bson:"state"`
	Zip       string `json:"zip" bson:"zip"`
}

type Owner struct {
	OwnerId   string `json:"ownerId" bson:"ownerId"`
	FirstName string `json:"firstName" bson:"firstName"`
	LastName  string `json:"lastName" bson:"lastName"`
	Dob       string `json:"dob" bson:"dob"`
}

type Chef struct {
	ChefId     string `json:"chefId" bson:"chefId"`
	FirstName  string `json:"firstName" bson:"firstName"`
	LastName   string `json:"lastName" bson:"lastName"`
	Dob        string `json:"dob" bson:"dob"`
	IsHeadChef bool   `json:"isHeadChef" bson:"isHeadChef"`
}

type MenuItem struct {
	Type     string `json:"type" bson:"type"`
	DishName string `json:"dishName" bson:"dishName"`
	Price    struct {
		Dollars int `json:"dollars" bson:"dollars"`
		Cents   int `json:"cents" bson:"cents"`
	} `json:"price" bson:"price"`
}
