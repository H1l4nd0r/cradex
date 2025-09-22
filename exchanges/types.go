package exchanges

type PriceData struct {
	Symbol    string
	Source    string
	Price     float64
	Timestamp int64
}

type OrderbookData struct {
	Symbol    string
	Source    string
	BestBid   float64
	BestAsk   float64
	Timestamp int64
}

type TradeData struct {
	Symbol    string
	Source    string
	Price     float64
	Quantity  string
	Side      string // "buy" or "sell" (normalized)
	Timestamp int64
}
