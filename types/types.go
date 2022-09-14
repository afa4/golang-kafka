package types

type IsEvenResponse struct {
	RequestedAt int64
	IsEven      string
}

type IsEvenRequest struct {
	RequesterId string
	Integer     int32
	CreatedAt   int64
}
