package exchanges

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type BybitFuturesTrade struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		Symbol    string `json:"s"`
		Price     string `json:"p"`
		Size      string `json:"v"`
		Side      string `json:"S"`
		Timestamp int64  `json:"T"`
		TradeID   string `json:"i"`
	} `json:"data"`
}

type BybitFuturesOrderbook struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Symbol string     `json:"s"`
		Bids   [][]string `json:"b"`
		Asks   [][]string `json:"a"`
		UpdateID int64    `json:"u"`
		SeqNum   int64    `json:"seq"`
	} `json:"data"`
}

func ConnectBybitFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://stream.bybit.com/v5/public/linear"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Bybit futures connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Bybit futures WebSocket")

		subscribeMsg := map[string]interface{}{
			"op":   "subscribe",
			"args": make([]string, len(symbols)*2),
		}

		for i, symbol := range symbols {
			subscribeMsg["args"].([]string)[i*2] = fmt.Sprintf("orderbook.1.%s", symbol)
			subscribeMsg["args"].([]string)[i*2+1] = fmt.Sprintf("publicTrade.%s", symbol)
		}

		err = conn.WriteJSON(subscribeMsg)
		if err != nil {
			log.Printf("Bybit futures subscription error: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var message json.RawMessage
			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("Bybit futures read error: %v", err)
				conn.Close()
				break
			}

			// Try to parse as orderbook first
			var orderbookMsg BybitFuturesOrderbook
			if err := json.Unmarshal(message, &orderbookMsg); err == nil && 
			   len(orderbookMsg.Data.Asks) > 0 && len(orderbookMsg.Data.Bids) > 0 {
				
				bidPrice, err1 := strconv.ParseFloat(orderbookMsg.Data.Bids[0][0], 64)
				askPrice, err2 := strconv.ParseFloat(orderbookMsg.Data.Asks[0][0], 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    orderbookMsg.Data.Symbol,
					Source:    "bybit_futures",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: time.Now().UnixMilli(),
				}

				orderbookChan <- orderbookData
				continue
			}

			// Try to parse as trade message
			var tradeMsg BybitFuturesTrade
			if err := json.Unmarshal(message, &tradeMsg); err == nil && 
			   (tradeMsg.Type == "snapshot" || tradeMsg.Type == "delta") {
				
				for _, trade := range tradeMsg.Data {
					price, err := strconv.ParseFloat(trade.Price, 64)
					if err != nil {
						continue
					}

					// Normalize trade side (Bybit uses "Buy" and "Sell")
					var side string
					if trade.Side == "Buy" {
						side = "buy"
					} else {
						side = "sell"
					}

					tradeData := TradeData{
						Symbol:    trade.Symbol,
						Source:    "bybit_futures",
						Price:     price,
						Quantity:  trade.Size,
						Side:      side,
						Timestamp: trade.Timestamp,
					}

					tradeChan <- tradeData
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// BybitSpotTrade represents the structure for Bybit spot trade data
type BybitSpotTrade struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		Symbol    string `json:"s"`
		Price     string `json:"p"`
		Size      string `json:"v"`
		Side      string `json:"S"`
		Timestamp int64  `json:"T"`
		TradeID   string `json:"i"`
	} `json:"data"`
}

type BybitSpotOrderbook struct {
	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  struct {
		Symbol string     `json:"s"`
		Bids   [][]string `json:"b"`
		Asks   [][]string `json:"a"`
		UpdateID int64    `json:"u"`
		SeqNum   int64    `json:"seq"`
	} `json:"data"`
}

// ConnectBybitSpot connects to Bybit spot trading WebSocket API
func ConnectBybitSpot(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://stream.bybit.com/v5/public/spot"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Bybit spot connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Bybit spot WebSocket")

		subscribeMsg := map[string]interface{}{
			"op":   "subscribe",
			"args": make([]string, len(symbols)*2),
		}

		for i, symbol := range symbols {
			subscribeMsg["args"].([]string)[i*2] = fmt.Sprintf("orderbook.1.%s", symbol)
			subscribeMsg["args"].([]string)[i*2+1] = fmt.Sprintf("publicTrade.%s", symbol)
		}

		err = conn.WriteJSON(subscribeMsg)
		if err != nil {
			log.Printf("Bybit spot subscription error: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var message json.RawMessage
			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("Bybit spot read error: %v", err)
				conn.Close()
				break
			}

			// Try to parse as orderbook first
			var orderbookMsg BybitSpotOrderbook
			if err := json.Unmarshal(message, &orderbookMsg); err == nil && 
			   len(orderbookMsg.Data.Asks) > 0 && len(orderbookMsg.Data.Bids) > 0 {
				
				bidPrice, err1 := strconv.ParseFloat(orderbookMsg.Data.Bids[0][0], 64)
				askPrice, err2 := strconv.ParseFloat(orderbookMsg.Data.Asks[0][0], 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    orderbookMsg.Data.Symbol,
					Source:    "bybit_spot",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: time.Now().UnixMilli(),
				}

				orderbookChan <- orderbookData
				continue
			}

			// Try to parse as trade message
			var tradeMsg BybitSpotTrade
			if err := json.Unmarshal(message, &tradeMsg); err == nil && 
			   (tradeMsg.Type == "snapshot" || tradeMsg.Type == "delta") {
				
				for _, trade := range tradeMsg.Data {
					price, err := strconv.ParseFloat(trade.Price, 64)
					if err != nil {
						continue
					}

					// Normalize trade side (Bybit uses "Buy" and "Sell")
					var side string
					if trade.Side == "Buy" {
						side = "buy"
					} else {
						side = "sell"
					}

					tradeData := TradeData{
						Symbol:    trade.Symbol,
						Source:    "bybit_spot",
						Price:     price,
						Quantity:  trade.Size,
						Side:      side,
						Timestamp: trade.Timestamp,
					}

					tradeChan <- tradeData
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}
