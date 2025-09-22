package exchanges

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// PythPriceData represents the price information within a Pyth update
type PythPriceData struct {
	Price       string `json:"price"`
	Conf        string `json:"conf"`
	Expo        int    `json:"expo"`
	PublishTime int64  `json:"publish_time"`
}

// PythParsedFeed represents a single parsed price feed
type PythParsedFeed struct {
	ID       string        `json:"id"`
	Price    PythPriceData `json:"price"`
	EMAPrice PythPriceData `json:"ema_price"`
}

// PythSSEResponse represents the complete SSE response structure
type PythSSEResponse struct {
	Parsed []PythParsedFeed `json:"parsed"`
}

// Pyth price feed IDs for different symbols
var pythPriceFeedIDs = map[string]string{
	"BTCUSDT": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43", // BTC/USD price feed ID
}

// ParsePythPrice converts Pyth price string and exponent to float64
func ParsePythPrice(priceStr string, expo int) (float64, error) {
	priceInt, err := strconv.ParseInt(priceStr, 10, 64)
	if err != nil {
		return 0, err
	}
	realPrice := float64(priceInt) * math.Pow10(expo)
	return realPrice, nil
}

// ConnectPythPrices connects to Pyth Network SSE endpoint for price feeds
func ConnectPythPrices(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	// Filter symbols to only those we have price feed IDs for
	var validSymbols []string
	var priceFeedIDs []string
	
	for _, symbol := range symbols {
		if feedID, exists := pythPriceFeedIDs[symbol]; exists {
			validSymbols = append(validSymbols, symbol)
			priceFeedIDs = append(priceFeedIDs, feedID)
		}
	}
	
	if len(priceFeedIDs) == 0 {
		log.Printf("No valid Pyth price feed IDs found for symbols: %v", symbols)
		return
	}
		
	// Create the SSE URL with price feed IDs using array format
	var idParams []string
	for _, id := range priceFeedIDs {
		idParams = append(idParams, fmt.Sprintf("ids[]=%s", id))
	}
	idsParam := strings.Join(idParams, "&")
	sseURL := fmt.Sprintf("https://hermes.pyth.network/v2/updates/price/stream?%s", idsParam)
	
	for {		
		resp, err := http.Get(sseURL)
		if err != nil {
			log.Printf("Pyth SSE connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		log.Printf("Connected to Pyth SSE")
		
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			
			// SSE format: lines starting with "data:" contain the JSON data
			if strings.HasPrefix(line, "data:") {
				data := strings.TrimPrefix(line, "data:")
				data = strings.TrimSpace(data)
				
				// Skip empty data lines or heartbeat messages
				if data == "" || data == "heartbeat" {
					continue
				}
				
				var response PythSSEResponse
				if err := json.Unmarshal([]byte(data), &response); err != nil {
					log.Printf("Pyth JSON unmarshal error: %v", err)
					continue
				}
				
				// Process each parsed price feed
				for _, feed := range response.Parsed {
					// Find the symbol for this price feed ID
					var symbol string
					for sym, feedID := range pythPriceFeedIDs {
						if feedID == feed.ID {
							symbol = sym
							break
						}
					}
					
					if symbol == "" {
						continue
					}
					
					// Parse the price using the exponent
					price, err := ParsePythPrice(feed.Price.Price, feed.Price.Expo)
					if err != nil {
						log.Printf("Pyth price parsing error for %s: %v", symbol, err)
						continue
					}
										
					// Create price data
					priceData := PriceData{
						Symbol:    symbol,
						Source:    "pyth",
						Price:     price,
						Timestamp: feed.Price.PublishTime * 1000, // Convert to milliseconds
					}
					
					priceChan <- priceData
				}
			}
		}
		
		if err := scanner.Err(); err != nil {
			log.Printf("Pyth SSE scanner error: %v", err)
		}
		
		resp.Body.Close()
		log.Printf("Pyth SSE connection closed, reconnecting in 5 seconds...")
		time.Sleep(5 * time.Second)
	}
}