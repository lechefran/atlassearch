package model

type SearchOptions struct {
	Explain  bool   `json:"explain"`
	ScanType string `json:"scanType"` // column, index, or specific index name
}
