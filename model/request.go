package model

type SearchRequest struct {
	Id        string `json:"id" bson:"restaurantId,omitempty"`
	FirstName string `json:"firstName" bson:"owner.firstName,omitempty"`
	LastName  string `json:"lastName" bson:"owner.lastName,omitempty"`
	City      string `json:"city" bson:"address.city,omitempty"`
	State     string `json:"state" bson:"address.state,omitempty"`
	Country   string `json:"country" bson:"address.country,omitempty"`
}

type InstallRequest struct {
	Install string `json:"install"` // full or dummy
}
