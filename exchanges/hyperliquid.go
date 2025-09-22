package exchanges

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type HyperliquidTrade struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type HyperliquidTradeData struct {
	Coin      string  `json:"coin"`
	Price     string  `json:"px"`
	Size      string  `json:"sz"`
	Side      string  `json:"side"`
	Timestamp int64   `json:"time"`
}

type HyperliquidL2Book struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type HyperliquidLevel struct {
	Price string `json:"px"`
	Size  string `json:"sz"`
	Count int    `json:"n"`
}

type HyperliquidL2BookData struct {
	Coin   string              `json:"coin"`
	Levels [][]HyperliquidLevel `json:"levels"`
	Time   int64               `json:"time"`
}

func ConnectHyperliquidFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://api.hyperliquid.xyz/ws"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Hyperliquid connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Hyperliquid futures WebSocket")

		// Subscribe to trades and l2Book for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to BTC for Hyperliquid
			coin := symbol[:3] // Extract first 3 characters (BTC from BTCUSDT)

			// Subscribe to trades
			tradeSubscribeMsg := map[string]interface{}{
				"method": "subscribe",
				"subscription": map[string]interface{}{
					"type": "trades",
					"coin": coin,
				},
			}

			err = conn.WriteJSON(tradeSubscribeMsg)
			if err != nil {
				log.Printf("Hyperliquid trade subscription error for %s: %v", coin, err)
				continue
			}

			// Subscribe to l2Book (orderbook)
			l2BookSubscribeMsg := map[string]interface{}{
				"method": "subscribe",
				"subscription": map[string]interface{}{
					"type": "l2Book",
					"coin": coin,
				},
			}

			err = conn.WriteJSON(l2BookSubscribeMsg)
			if err != nil {
				log.Printf("Hyperliquid l2Book subscription error for %s: %v", coin, err)
				continue
			}
		}

		for {
			var message json.RawMessage
			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("Hyperliquid read error: %v", err)
				conn.Close()
				break
			}

			// Try to parse as trade message first
			var tradeMessage HyperliquidTrade
			if err := json.Unmarshal(message, &tradeMessage); err == nil && tradeMessage.Channel == "trades" && len(tradeMessage.Data) > 0 {
				// Handle both array and single object formats
				var trades []HyperliquidTradeData
				
				// Try to unmarshal as array first
				if err := json.Unmarshal(tradeMessage.Data, &trades); err != nil {
					// If that fails, try as single object
					var singleTrade HyperliquidTradeData
					if err := json.Unmarshal(tradeMessage.Data, &singleTrade); err != nil {
						log.Printf("Hyperliquid trade data parse error: %v", err)
						continue
					}
					trades = []HyperliquidTradeData{singleTrade}
				}

				for _, trade := range trades {
					// Parse price from string
					price, err := strconv.ParseFloat(trade.Price, 64)
					if err != nil {
						continue
					}

					// Convert coin back to symbol format (BTC -> BTCUSDT)
					symbol := trade.Coin + "USDT"

					// Normalize trade side (Hyperliquid uses "A" for ask/sell, "B" for bid/buy)
					var side string
					if trade.Side == "B" {
						side = "buy"
					} else {
						side = "sell"
					}

					tradeData := TradeData{
						Symbol:    symbol,
						Source:    "hyperliquid_futures",
						Price:     price,
						Quantity:  trade.Size,
						Side:      side,
						Timestamp: trade.Timestamp,
					}

					tradeChan <- tradeData
				}
				continue
			}

			// Try to parse as l2Book message
			var l2BookMessage HyperliquidL2Book
			if err := json.Unmarshal(message, &l2BookMessage); err == nil && l2BookMessage.Channel == "l2Book" && len(l2BookMessage.Data) > 0 {
				var l2BookData HyperliquidL2BookData
				if err := json.Unmarshal(l2BookMessage.Data, &l2BookData); err != nil {
					log.Printf("Hyperliquid l2Book data parse error: %v", err)
					continue
				}

				if len(l2BookData.Levels) >= 2 && len(l2BookData.Levels[0]) > 0 && len(l2BookData.Levels[1]) > 0 {
					// Hyperliquid l2Book format: levels[0] is bids, levels[1] is asks
					// Each level has px (price), sz (size), n (count)
					bestBid, err1 := strconv.ParseFloat(l2BookData.Levels[0][0].Price, 64)
					bestAsk, err2 := strconv.ParseFloat(l2BookData.Levels[1][0].Price, 64)
					if err1 != nil || err2 != nil {
						continue
					}

					// Convert coin back to symbol format (BTC -> BTCUSDT)
					symbol := l2BookData.Coin + "USDT"

					orderbookData := OrderbookData{
						Symbol:    symbol,
						Source:    "hyperliquid_futures",
						BestBid:   bestBid,
						BestAsk:   bestAsk,
						Timestamp: l2BookData.Time,
					}

					orderbookChan <- orderbookData
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}
