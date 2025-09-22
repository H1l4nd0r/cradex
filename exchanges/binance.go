package exchanges

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type BinanceFuturesTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"a"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
}

type BinanceFuturesBookTicker struct {
	EventType   string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

func ConnectBinanceFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	streamNames := make([]string, len(symbols)*2)
	for i, symbol := range symbols {
		streamNames[i*2] = strings.ToLower(symbol) + "@bookTicker"
		streamNames[i*2+1] = strings.ToLower(symbol) + "@aggTrade"
	}
	streamParam := strings.Join(streamNames, "/")

	wsURL := fmt.Sprintf("wss://fstream.binance.com/stream?streams=%s", streamParam)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Binance futures connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Binance futures WebSocket")

		for {
			var message struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}

			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("Binance futures read error: %v", err)
				conn.Close()
				break
			}

			if strings.Contains(message.Stream, "@bookTicker") {
				var bookTicker BinanceFuturesBookTicker
				if err := json.Unmarshal(message.Data, &bookTicker); err != nil {
					continue
				}

				bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBidPrice, 64)
				askPrice, err2 := strconv.ParseFloat(bookTicker.BestAskPrice, 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    bookTicker.Symbol,
					Source:    "binance_futures",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: bookTicker.EventTime,
				}

				orderbookChan <- orderbookData

			} else if strings.Contains(message.Stream, "@aggTrade") {
				var trade BinanceFuturesTrade
				if err := json.Unmarshal(message.Data, &trade); err != nil {
					continue
				}

				price, err := strconv.ParseFloat(trade.Price, 64)
				if err != nil {
					continue
				}

				// Normalize trade side (isMaker: false = buy aggressor, true = sell aggressor)
				var side string
				if !trade.IsMaker {
					side = "buy"
				} else {
					side = "sell"
				}

				tradeData := TradeData{
					Symbol:    trade.Symbol,
					Source:    "binance_futures",
					Price:     price,
					Quantity:  trade.Quantity,
					Side:      side,
					Timestamp: trade.TradeTime,
				}

				tradeChan <- tradeData
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// BinanceSpotTrade represents the structure for Binance spot trade data
type BinanceSpotTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"a"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
}

type BinanceSpotBookTicker struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

// ConnectBinanceSpot connects to Binance spot trading WebSocket API
func ConnectBinanceSpot(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	streamNames := make([]string, len(symbols)*2)
	for i, symbol := range symbols {
		streamNames[i*2] = strings.ToLower(symbol) + "@bookTicker"
		streamNames[i*2+1] = strings.ToLower(symbol) + "@aggTrade"
	}
	streamParam := strings.Join(streamNames, "/")

	wsURL := fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s", streamParam)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Binance spot connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Binance spot WebSocket")

		for {
			var message struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}

			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("Binance spot read error: %v", err)
				conn.Close()
				break
			}

			if strings.Contains(message.Stream, "@bookTicker") {
				var bookTicker BinanceSpotBookTicker
				if err := json.Unmarshal(message.Data, &bookTicker); err != nil {
					continue
				}

				bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBidPrice, 64)
				askPrice, err2 := strconv.ParseFloat(bookTicker.BestAskPrice, 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    bookTicker.Symbol,
					Source:    "binance_spot",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: bookTicker.EventTime,
				}

				orderbookChan <- orderbookData

			} else if strings.Contains(message.Stream, "@aggTrade") {
				var trade BinanceSpotTrade
				if err := json.Unmarshal(message.Data, &trade); err != nil {
					continue
				}

				price, err := strconv.ParseFloat(trade.Price, 64)
				if err != nil {
					continue
				}

				// Normalize trade side (isMaker: false = buy aggressor, true = sell aggressor)
				var side string
				if !trade.IsMaker {
					side = "buy"
				} else {
					side = "sell"
				}

				tradeData := TradeData{
					Symbol:    trade.Symbol,
					Source:    "binance_spot",
					Price:     price,
					Quantity:  trade.Quantity,
					Side:      side,
					Timestamp: trade.TradeTime,
				}

				tradeChan <- tradeData
			}
		}

		time.Sleep(2 * time.Second)
	}
}
