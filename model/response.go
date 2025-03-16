package model

type StatusResponse struct {
	Code  int    `json:"code"`
	Title string `json:"title"`
	Msg   string `json:"message"`
}
