package pinecone

type QueryReq struct {
	Namespace       string    `json:"namespace"`
	TopK            int64     `json:"topK"`
	Vector          []float64 `json:"vector"`
	IncludeValues   bool      `json:"includeValues"`
	IncludeMetadata bool      `json:"includeMetadata"`
	ID              string    `json:"id"`
}

type QueryResp struct {
	Namespace string  `json:"namespace"`
	Matches   []Match `json:"matches"`
}

type Match struct {
	ID     string    `json:"id"`
	Score  float64   `json:"score"`
	Values []float64 `json:"values"`
}

type UpsertReq struct {
	Vectors []Vector `json:"vectors"`
}

type Vector struct {
	ID     string    `json:"id"`
	Values []float64 `json:"values"`
}

type UpsertResp struct {
	RecordsUpserted int64 `json:"upsertedCount"`
}
