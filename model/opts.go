package model

type SearchOptions struct {
	Explain bool `json:"explain"`
}

type ParameterOptions struct {
	SearchIndex string `json:"searchIndex"`
}
