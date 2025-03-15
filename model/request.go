package model

type Request struct {
	Id       string `json:"id"`
	HeadChef struct {
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	} `json:"headChef"`
	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
}
