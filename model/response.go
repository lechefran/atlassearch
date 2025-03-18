package model

type StatusResponse struct {
	Code  int    `json:"code"`
	Title string `json:"title"`
	Msg   string `json:"message"`
}

type RestaurantResponse struct {
	Status   Status       `json:"status"`
	Response []Restaurant `json:"response"`
}

type Status struct {
	Code int    `json:"code"`
	Msg  string `json:"message"`
}
