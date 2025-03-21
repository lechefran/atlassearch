package model

type InstallRequest struct {
	Install       string `json:"install"` // full or dummy
	LoadIndexes   bool   `json:"loadIndexes"`
	DocumentCount int64  `json:"documentCount"`
}
