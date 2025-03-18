package model

type SearchOptions struct {
	Explain     bool        `json:"explain"`
	ScanType    string      `json:"scanType"`    // column or index
	SearchIndex interface{} `json:"searchIndex"` // specific index name
}

type ParameterOptions struct {
	IsAtlasSearchQuery bool   `json:"isAtlasSearchQuery"`
	SearchIndex        string `json:"searchIndex"`
}
