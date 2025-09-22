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

type OKXFuturesTrade struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID    string `json:"instId"`
		TradeID   string `json:"tradeId"`
		Price     string `json:"px"`
		Size      string `json:"sz"`
		Side      string `json:"side"`
		Timestamp string `json:"ts"`
	} `json:"data"`
}

type OKXFuturesOrderbook struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID    string     `json:"instId"`
		Bids      [][]string `json:"bids"`
		Asks      [][]string `json:"asks"`
		Timestamp string     `json:"ts"`
	} `json:"data"`
}

type OKXSubscribeMessage struct {
	Op   string `json:"op"`
	Args []struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"args"`
}

func ConnectOKXFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://ws.okx.com:8443/ws/v5/public"

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("OKX connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to OKX futures WebSocket")

		// Subscribe to both trades and orderbooks for all symbols
		var subscribeArgs []struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		}

		for _, symbol := range symbols {
			// Convert symbol format (BTCUSDT -> BTC-USDT-SWAP for perpetual futures)
			okxSymbol := convertToOKXSymbol(symbol)
			
			// Subscribe to trades
			subscribeArgs = append(subscribeArgs, struct {
				Channel string `json:"channel"`
				InstID  string `json:"instId"`
			}{
				Channel: "trades",
				InstID:  okxSymbol,
			})
			
			// Subscribe to orderbooks (books5 for top 5 levels)
			subscribeArgs = append(subscribeArgs, struct {
				Channel string `json:"channel"`
				InstID  string `json:"instId"`
			}{
				Channel: "books5",
				InstID:  okxSymbol,
			})
		}

		subscribeMsg := OKXSubscribeMessage{
			Op:   "subscribe",
			Args: subscribeArgs,
		}

		err = conn.WriteJSON(subscribeMsg)
		if err != nil {
			log.Printf("OKX subscription error: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var message json.RawMessage
			err := conn.ReadJSON(&message)
			if err != nil {
				log.Printf("OKX read error: %v", err)
				conn.Close()
				break
			}

			// Check if it's a trade message
			var tradeMsg OKXFuturesTrade
			if err := json.Unmarshal(message, &tradeMsg); err == nil && tradeMsg.Arg.Channel == "trades" && len(tradeMsg.Data) > 0 {
				for _, trade := range tradeMsg.Data {
					price, err := strconv.ParseFloat(trade.Price, 64)
					if err != nil {
						continue
					}

					// Convert timestamp from string to int64
					timestamp, err := strconv.ParseInt(trade.Timestamp, 10, 64)
					if err != nil {
						timestamp = time.Now().UnixMilli()
					}

					// Convert OKX symbol back to standard format
					standardSymbol := convertFromOKXSymbol(trade.InstID)

					tradeData := TradeData{
						Symbol:    standardSymbol,
						Source:    "okx_futures",
						Price:     price,
						Quantity:  trade.Size,
						Side:      trade.Side, // OKX already provides "buy" or "sell"
						Timestamp: timestamp,
					}

					tradeChan <- tradeData
				}
				continue
			}

			// Check if it's an orderbook message
			var orderbookMsg OKXFuturesOrderbook
			if err := json.Unmarshal(message, &orderbookMsg); err == nil && orderbookMsg.Arg.Channel == "books5" && len(orderbookMsg.Data) > 0 {
				for _, book := range orderbookMsg.Data {
					if len(book.Bids) == 0 || len(book.Asks) == 0 {
						continue
					}

					// Parse best bid and ask
					bestBid, err1 := strconv.ParseFloat(book.Bids[0][0], 64)
					bestAsk, err2 := strconv.ParseFloat(book.Asks[0][0], 64)
					if err1 != nil || err2 != nil {
						continue
					}

					// Convert timestamp from string to int64
					timestamp, err := strconv.ParseInt(book.Timestamp, 10, 64)
					if err != nil {
						timestamp = time.Now().UnixMilli()
					}

					// Convert OKX symbol back to standard format
					standardSymbol := convertFromOKXSymbol(book.InstID)

					orderbookData := OrderbookData{
						Symbol:    standardSymbol,
						Source:    "okx_futures",
						BestBid:   bestBid,
						BestAsk:   bestAsk,
						Timestamp: timestamp,
					}

					orderbookChan <- orderbookData
				}
				continue
			}
		}

		time.Sleep(2 * time.Second)
	}
}

// convertToOKXSymbol converts standard symbol format to OKX format
// BTCUSDT -> BTC-USDT-SWAP (for perpetual futures)
func convertToOKXSymbol(symbol string) string {
	// Handle common symbols
	switch symbol {
	case "BTCUSDT":
		return "BTC-USDT-SWAP"
	case "ETHUSDT":
		return "ETH-USDT-SWAP"
	case "ADAUSDT":
		return "ADA-USDT-SWAP"
	case "SOLUSDT":
		return "SOL-USDT-SWAP"
	case "DOTUSDT":
		return "DOT-USDT-SWAP"
	case "LINKUSDT":
		return "LINK-USDT-SWAP"
	case "AVAXUSDT":
		return "AVAX-USDT-SWAP"
	case "MATICUSDT":
		return "MATIC-USDT-SWAP"
	case "UNIUSDT":
		return "UNI-USDT-SWAP"
	case "LTCUSDT":
		return "LTC-USDT-SWAP"
	case "BCHUSDT":
		return "BCH-USDT-SWAP"
	case "XRPUSDT":
		return "XRP-USDT-SWAP"
	default:
		// Generic conversion for other symbols
		if strings.HasSuffix(symbol, "USDT") {
			base := strings.TrimSuffix(symbol, "USDT")
			return fmt.Sprintf("%s-USDT-SWAP", base)
		}
		return symbol + "-SWAP"
	}
}

// convertFromOKXSymbol converts OKX symbol format back to standard format
// BTC-USDT-SWAP -> BTCUSDT
func convertFromOKXSymbol(okxSymbol string) string {
	// Remove -SWAP suffix and convert to standard format
	if strings.HasSuffix(okxSymbol, "-SWAP") {
		withoutSwap := strings.TrimSuffix(okxSymbol, "-SWAP")
		parts := strings.Split(withoutSwap, "-")
		if len(parts) == 2 {
			return parts[0] + parts[1]
		}
	}
	return okxSymbol
}