package model

type SearchRequest struct {
	Id       string `json:"id"`
	HeadChef struct {
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
	} `json:"headChef"`
	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
	Explain string `json:"explanation"`
}

type InstallRequest struct {
	Install string `json:"install"` // full or dummy
}
