package exchanges

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type KrakenOrderBookEntry struct {
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
}

type KrakenOrderBookData struct {
	Feed      string                 `json:"feed"`
	ProductID string                 `json:"product_id"`
	Side      string                 `json:"side,omitempty"`
	Seq       int64                  `json:"seq"`
	Price     float64                `json:"price,omitempty"`
	Qty       float64                `json:"qty,omitempty"`
	Bids      []KrakenOrderBookEntry `json:"bids,omitempty"`
	Asks      []KrakenOrderBookEntry `json:"asks,omitempty"`
	Timestamp float64                `json:"timestamp,omitempty"`
}

type KrakenOrderBook struct {
	Bids []KrakenOrderBookEntry
	Asks []KrakenOrderBookEntry
}

func processKrakenOrderbook(productID string, orderBook *KrakenOrderBook, orderbookChan chan<- OrderbookData) {
	if len(orderBook.Bids) == 0 || len(orderBook.Asks) == 0 {
		return
	}

	// Convert symbol back to standard format (PF_XBTUSD -> BTCUSDT)
	symbol := convertFromKrakenSymbol(productID)

	// Get best bid (highest price in bids)
	bestBid := orderBook.Bids[0].Price

	// Get best ask (lowest price in asks)
	bestAsk := orderBook.Asks[0].Price

	orderbookData := OrderbookData{
		Symbol:    symbol,
		Source:    "kraken_futures",
		BestBid:   bestBid,
		BestAsk:   bestAsk,
		Timestamp: time.Now().UnixMilli(),
	}

	orderbookChan <- orderbookData
}

func ConnectKrakenFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://futures.kraken.com/ws/v1"

	// Maintain orderbooks for each symbol
	orderbooks := make(map[string]*KrakenOrderBook)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Kraken connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Kraken futures WebSocket")

		// Subscribe to orderbook for each symbol
		for _, symbol := range symbols {
			// Convert BTCUSDT to PF_XBTUSD for Kraken
			krakenSymbol := convertToKrakenSymbol(symbol)

			subscribeMsg := map[string]interface{}{
				"event":       "subscribe",
				"feed":        "book",
				"product_ids": []string{krakenSymbol},
			}

			err = conn.WriteJSON(subscribeMsg)
			if err != nil {
				log.Printf("Kraken subscription error for %s: %v", krakenSymbol, err)
				continue
			}

			// Initialize orderbook
			orderbooks[krakenSymbol] = &KrakenOrderBook{
				Bids: make([]KrakenOrderBookEntry, 0),
				Asks: make([]KrakenOrderBookEntry, 0),
			}
		}

		for {
			var rawMessage map[string]interface{}
			err := conn.ReadJSON(&rawMessage)
			if err != nil {
				log.Printf("Kraken read error: %v", err)
				conn.Close()
				break
			}

			// Check if it's a book_snapshot or book update
			if feed, ok := rawMessage["feed"].(string); ok {
				var data KrakenOrderBookData
				messageBytes, _ := json.Marshal(rawMessage)
				err = json.Unmarshal(messageBytes, &data)
				if err != nil {
					log.Printf("Kraken orderbook unmarshal error: %v", err)
					continue
				}

				orderbook, exists := orderbooks[data.ProductID]
				if !exists {
					continue
				}

				if feed == "book_snapshot" {
					// Initial snapshot
					orderbook.Bids = data.Bids
					orderbook.Asks = data.Asks
				} else if feed == "book" {
					// Incremental update
					updateKrakenOrderbook(orderbook, data)
				}

				// Send updated orderbook
				processKrakenOrderbook(data.ProductID, orderbook, orderbookChan)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func updateKrakenOrderbook(orderbook *KrakenOrderBook, data KrakenOrderBookData) {
	if data.Qty == 0 {
		// Remove price level
		removePriceLevel(orderbook, data.Side, data.Price)
	} else {
		// Update or add price level
		upsertPriceLevel(orderbook, data.Side, data.Price, data.Qty)
	}
}

func removePriceLevel(orderbook *KrakenOrderBook, side string, price float64) {
	if side == "buy" {
		for i, entry := range orderbook.Bids {
			if entry.Price == price {
				orderbook.Bids = append(orderbook.Bids[:i], orderbook.Bids[i+1:]...)
				break
			}
		}
	} else if side == "sell" {
		for i, entry := range orderbook.Asks {
			if entry.Price == price {
				orderbook.Asks = append(orderbook.Asks[:i], orderbook.Asks[i+1:]...)
				break
			}
		}
	}
}

func upsertPriceLevel(orderbook *KrakenOrderBook, side string, price, qty float64) {
	newEntry := KrakenOrderBookEntry{Price: price, Qty: qty}

	if side == "buy" {
		// Bids are sorted in descending order (highest first)
		for i, entry := range orderbook.Bids {
			if entry.Price == price {
				// Update existing level
				orderbook.Bids[i].Qty = qty
				return
			}

			if price > entry.Price {
				// Insert new level
				orderbook.Bids = append(orderbook.Bids[:i], append([]KrakenOrderBookEntry{newEntry}, orderbook.Bids[i:]...)...)
				return
			}
		}
		// Append to end if not inserted
		orderbook.Bids = append(orderbook.Bids, newEntry)
	} else if side == "sell" {
		// Asks are sorted in ascending order (lowest first)
		for i, entry := range orderbook.Asks {
			if entry.Price == price {
				// Update existing level
				orderbook.Asks[i].Qty = qty
				return
			}

			if price < entry.Price {
				// Insert new level
				orderbook.Asks = append(orderbook.Asks[:i], append([]KrakenOrderBookEntry{newEntry}, orderbook.Asks[i:]...)...)
				return
			}
		}
		// Append to end if not inserted
		orderbook.Asks = append(orderbook.Asks, newEntry)
	}
}

func convertToKrakenSymbol(symbol string) string {
	// Convert BTCUSDT to PF_XBTUSD (Kraken's format for perpetual futures)
	switch symbol {
	case "BTCUSDT":
		return "PF_XBTUSD"
	case "ETHUSDT":
		return "PF_ETHUSD"
	case "XRPUSDT":
		return "PF_XRPUSD"
	case "SOLUSDT":
		return "PF_SOLUSD"
	default:
		return symbol
	}
}

func convertFromKrakenSymbol(symbol string) string {
	// Convert PF_XBTUSD to BTCUSDT (standard format)
	switch symbol {
	case "PF_XBTUSD":
		return "BTCUSDT"
	case "PF_ETHUSD":
		return "ETHUSDT"
	case "PF_XRPUSD":
		return "XRPUSDT"
	case "PF_SOLUSD":
		return "SOLUSDT"
	default:
		return symbol
	}
}