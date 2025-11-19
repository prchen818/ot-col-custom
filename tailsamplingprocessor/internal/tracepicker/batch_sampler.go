package tracepicker

type BatchSampler struct {
	Encoder *BFSEncoder
}

func NewBatchSampler() *BatchSampler {
	histPool := NewHistPool(200)
	return &BatchSampler{
		Encoder: NewBFSEncoder(histPool),
	}
}
