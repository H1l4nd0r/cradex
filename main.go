package main

import (
	"encoding/json"
	"fmt"
	"futures-arbitrage-scanner/exchanges"

	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

type ArbitrageOpportunity struct {
	Symbol     string  `json:"symbol"`
	BuySource  string  `json:"buy_source"`
	SellSource string  `json:"sell_source"`
	BuyPrice   float64 `json:"buy_price"`
	SellPrice  float64 `json:"sell_price"`
	ProfitPct  float64 `json:"profit_pct"`
	Timestamp  int64   `json:"timestamp"`
	Id         string  `json:"id,omitempty"`
}

type ActiveOrder struct {
	Id          string                `json:"id"`
	Symbol      string                `json:"symbol"`
	Side        string                `json:"side"` // "buy" or "sell"
	Source      string                `json:"source"`
	Price       float64               `json:"price"`
	Quantity    float64               `json:"quantity"`
	Status      string                `json:"status"` // "pending", "filled", "cancelled", "closed"
	Timestamp   int64                 `json:"timestamp"`
	Opportunity *ArbitrageOpportunity `json:"opportunity,omitempty"`
}

type FuturesScanner struct {
	prices            map[string]map[string]float64
	pricesMutex       sync.RWMutex
	activeOrders      []ActiveOrder
	ordersMutex       sync.RWMutex
	opportunities     map[string]ArbitrageOpportunity
	oppMutex          sync.RWMutex
	wsClients         map[*websocket.Conn]bool // Main socket clients (market data)
	clientsMutex      sync.RWMutex
	orderWsClients    map[*websocket.Conn]bool // Order socket clients
	orderClientsMutex sync.RWMutex
	wsWriteMutex      sync.Mutex // Protects WebSocket writes
	upgrader          websocket.Upgrader
	priceChan         chan exchanges.PriceData
	orderbookChan     chan exchanges.OrderbookData
	tradeChan         chan exchanges.TradeData
	orderCommandChan  chan string          // Channel for order commands
	lastOpportunity   map[string]time.Time // Track last alert per symbol
	opportunityMutex  sync.RWMutex
	minProfitFilter   float64      // Minimum profit percentage for arbitrage alerts
	minProfitMutex    sync.RWMutex // Protects minProfitFilter
}

func NewFuturesScanner() *FuturesScanner {
	return &FuturesScanner{
		prices:           make(map[string]map[string]float64),
		opportunities:    make(map[string]ArbitrageOpportunity),
		wsClients:        make(map[*websocket.Conn]bool),
		orderWsClients:   make(map[*websocket.Conn]bool),
		priceChan:        make(chan exchanges.PriceData, 1000),
		orderbookChan:    make(chan exchanges.OrderbookData, 1000),
		tradeChan:        make(chan exchanges.TradeData, 1000),
		orderCommandChan: make(chan string, 100),
		lastOpportunity:  make(map[string]time.Time),
		minProfitFilter:  0.15, // Default minimum profit filter
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

func (s *FuturesScanner) processPrices() {
	for priceData := range s.priceChan {
		s.updatePrice(priceData)
	}
}

func (s *FuturesScanner) processOrderbooks() {
	for orderbookData := range s.orderbookChan {
		// Calculate mid price from best bid and best ask
		midPrice := (orderbookData.BestBid + orderbookData.BestAsk) / 2

		priceData := exchanges.PriceData{
			Symbol:    orderbookData.Symbol,
			Source:    orderbookData.Source,
			Price:     midPrice,
			Timestamp: orderbookData.Timestamp,
		}

		s.updatePrice(priceData)
	}
}

func (s *FuturesScanner) processTrades() {
	for range s.tradeChan {
		// Keep trade data for future use but don't use for pricing
	}
}

func (s *FuturesScanner) processOrderCommands() {
	for message := range s.orderCommandChan {
		log.Printf("Processing order command from channel: %q", message)
		// Handle "execute_arbitrage" messages
		if strings.HasPrefix(message, "execute_arbitrage:") {
			// Extract opportunity ID from message
			// Format: "execute_arbitrage:opportunity_id"
			parts := strings.Split(message, ":")
			if len(parts) == 2 {
				opportunityId := strings.TrimSpace(parts[1])
				if opportunityId != "" {
					s.executeArbitrageOrders(opportunityId)
					log.Printf("Processed execute arbitrage for opportunity %s", opportunityId)
				} else {
					log.Printf("Invalid opportunity ID in message: %q", message)
				}
			} else {
				log.Printf("Invalid execute_arbitrage message format: %q", message)
			}
		}
		// Handle "close_order" messages
		if strings.HasPrefix(message, "close_order:") {
			// Extract order ID from message
			// Format: "close_order:order_id"
			parts := strings.Split(message, ":")
			if len(parts) == 2 {
				orderId := strings.TrimSpace(parts[1])
				if orderId != "" {
					s.closeOrder(orderId)
					log.Printf("Processed close order for order %s", orderId)
				} else {
					log.Printf("Invalid order ID in message: %q", message)
				}
			} else {
				log.Printf("Invalid close_order message format: %q", message)
			}
		}
		// Handle "clear_closed" messages
		if strings.HasPrefix(message, "clear_closed") {
			s.clearClosedOpportunities()
			log.Printf("Processed clear closed opportunities")
		}
		// Handle "min_profit_filter" messages
		if strings.HasPrefix(message, "min_profit_filter:") {
			// Extract min profit filter value from message
			// Format: "min_profit_filter:0.15"
			parts := strings.Split(message, ":")
			if len(parts) == 2 {
				var minProfit float64
				_, err := fmt.Sscanf(strings.TrimSpace(parts[1]), "%f", &minProfit)
				if err == nil && minProfit >= 0 {
					s.minProfitMutex.Lock()
					s.minProfitFilter = minProfit
					s.minProfitMutex.Unlock()
					log.Printf("Updated min profit filter to: %.3f%%", minProfit)
				} else {
					log.Printf("Invalid min profit filter value in message: %q", message)
				}
			} else {
				log.Printf("Invalid min_profit_filter message format: %q", message)
			}
		}
		// Add other order commands here if needed
	}
}

func (s *FuturesScanner) updatePrice(data exchanges.PriceData) {
	s.pricesMutex.Lock()
	if s.prices[data.Symbol] == nil {
		s.prices[data.Symbol] = make(map[string]float64)
	}
	s.prices[data.Symbol][data.Source] = data.Price
	s.pricesMutex.Unlock()

	s.checkArbitrage(data.Symbol)
}

func (s *FuturesScanner) checkArbitrage(symbol string) {
	s.pricesMutex.RLock()
	sourcePrices, exists := s.prices[symbol]
	if !exists || len(sourcePrices) < 2 {
		s.pricesMutex.RUnlock()
		return
	}

	// Create a copy of the prices map to avoid race conditions
	pricesCopy := make(map[string]float64)
	for source, price := range sourcePrices {
		pricesCopy[source] = price
	}
	s.pricesMutex.RUnlock()

	var minPrice, maxPrice float64
	var minSource, maxSource string
	first := true

	for source, price := range pricesCopy {
		if first {
			minPrice = price
			maxPrice = price
			minSource = source
			maxSource = source
			first = false
			continue
		}

		if price < minPrice {
			minPrice = price
			minSource = source
		}
		if price > maxPrice {
			maxPrice = price
			maxSource = source
		}
	}

	profitPct := ((maxPrice - minPrice) / minPrice) * 100

	// Get current min profit filter
	s.minProfitMutex.RLock()
	minProfitThreshold := s.minProfitFilter
	s.minProfitMutex.RUnlock()

	// Only alert if profit exceeds the threshold and we haven't alerted recently
	if profitPct > minProfitThreshold {
		opportunityKey := fmt.Sprintf("%s_%s_%s", symbol, minSource, maxSource)

		s.opportunityMutex.RLock()
		lastAlert, exists := s.lastOpportunity[opportunityKey]
		s.opportunityMutex.RUnlock()

		now := time.Now()
		// Only send alert if it's been more than 10 seconds since last alert for this pair
		// This prevents spam while still allowing frequent updates for crypto markets
		if !exists || now.Sub(lastAlert) > 10*time.Second {
			s.opportunityMutex.Lock()
			s.lastOpportunity[opportunityKey] = now
			s.opportunityMutex.Unlock()

			opportunity := ArbitrageOpportunity{
				Id:         fmt.Sprintf("%s_%d", symbol, now.UnixMilli()),
				Symbol:     symbol,
				BuySource:  minSource,
				SellSource: maxSource,
				BuyPrice:   minPrice,
				SellPrice:  maxPrice,
				ProfitPct:  profitPct,
				Timestamp:  now.UnixMilli(),
			}

			s.broadcastOpportunity(opportunity)
		}
	}

	// Always broadcast current spreads for the spread matrix using the copy
	s.broadcastSpreads(symbol, pricesCopy)
}

func (s *FuturesScanner) broadcastOpportunity(opportunity ArbitrageOpportunity) {
	s.oppMutex.Lock()
	s.opportunities[opportunity.Id] = opportunity
	s.oppMutex.Unlock()

	s.clientsMutex.RLock()
	clients := make([]*websocket.Conn, 0, len(s.wsClients))
	for client := range s.wsClients {
		clients = append(clients, client)
	}
	s.clientsMutex.RUnlock()

	message := map[string]interface{}{
		"type":        "arbitrage",
		"opportunity": opportunity,
	}

	s.wsWriteMutex.Lock()
	defer s.wsWriteMutex.Unlock()

	//log.Printf("Broadcasting arbitrage to %d clients", len(clients))

	var toRemove []*websocket.Conn
	for _, client := range clients {
		err := client.WriteJSON(message)
		if err != nil {
			log.Printf("WebSocket write error: %v", err)
			client.Close()
			toRemove = append(toRemove, client)
		} else {
			//log.Printf("Successfully sent arbitrage message to client")
		}
	}

	// Remove failed clients
	if len(toRemove) > 0 {
		s.clientsMutex.Lock()
		for _, client := range toRemove {
			delete(s.wsClients, client)
		}
		s.clientsMutex.Unlock()
		log.Printf("Removed %d failed WebSocket clients", len(toRemove))
	}
}

func (s *FuturesScanner) broadcastSpreads(symbol string, sourcePrices map[string]float64) {
	s.clientsMutex.RLock()
	clients := make([]*websocket.Conn, 0, len(s.wsClients))
	for client := range s.wsClients {
		clients = append(clients, client)
	}
	s.clientsMutex.RUnlock()

	// Calculate all pairwise spreads
	spreads := make(map[string]map[string]float64)

	for buySource, buyPrice := range sourcePrices {
		spreads[buySource] = make(map[string]float64)
		for sellSource, sellPrice := range sourcePrices {
			if buySource != sellSource {
				spreadPct := ((sellPrice - buyPrice) / buyPrice) * 100
				spreads[buySource][sellSource] = spreadPct
			}
		}
	}

	message := map[string]interface{}{
		"type":    "spreads",
		"symbol":  symbol,
		"spreads": spreads,
		"prices":  sourcePrices,
	}

	s.wsWriteMutex.Lock()
	defer s.wsWriteMutex.Unlock()

	var toRemove []*websocket.Conn
	for _, client := range clients {
		err := client.WriteJSON(message)
		if err != nil {
			log.Printf("WebSocket write error: %v", err)
			client.Close()
			toRemove = append(toRemove, client)
		}
	}

	// Remove failed clients
	if len(toRemove) > 0 {
		s.clientsMutex.Lock()
		for _, client := range toRemove {
			delete(s.wsClients, client)
		}
		s.clientsMutex.Unlock()
	}
}

func (s *FuturesScanner) broadcastPrices() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.pricesMutex.RLock()
		pricesCopy := make(map[string]map[string]float64)
		for symbol, prices := range s.prices {
			pricesCopy[symbol] = make(map[string]float64)
			for exchange, price := range prices {
				pricesCopy[symbol][exchange] = price
			}
		}
		s.pricesMutex.RUnlock()

		if len(pricesCopy) > 0 {
			message := map[string]interface{}{
				"type":   "prices",
				"prices": pricesCopy,
			}

			s.clientsMutex.RLock()
			clients := make([]*websocket.Conn, 0, len(s.wsClients))
			for client := range s.wsClients {
				clients = append(clients, client)
			}
			s.clientsMutex.RUnlock()

			s.wsWriteMutex.Lock()
			var toRemove []*websocket.Conn
			for _, client := range clients {
				err := client.WriteJSON(message)
				if err != nil {
					log.Printf("WebSocket write error: %v", err)
					client.Close()
					toRemove = append(toRemove, client)
				}
			}
			s.wsWriteMutex.Unlock()

			// Remove failed clients
			if len(toRemove) > 0 {
				s.clientsMutex.Lock()
				for _, client := range toRemove {
					delete(s.wsClients, client)
				}
				s.clientsMutex.Unlock()
			}
		}
	}
}

func (s *FuturesScanner) handleOrdersWebSocket(w http.ResponseWriter, r *http.Request) {
	log.Printf("Orders WebSocket connection attempt from %s", r.RemoteAddr)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Orders WebSocket upgrade error from %s: %v", r.RemoteAddr, err)
		return
	}
	defer conn.Close()

	s.orderClientsMutex.Lock()
	s.orderWsClients[conn] = true
	orderClientCount := len(s.orderWsClients)
	s.orderClientsMutex.Unlock()

	log.Printf("Orders WebSocket client connected from %s. Total order clients: %d", r.RemoteAddr, orderClientCount)

	// Send current active orders to the new client
	go s.broadcastActiveOrders()

	defer func() {
		s.orderClientsMutex.Lock()
		delete(s.orderWsClients, conn)
		log.Printf("Orders WebSocket client disconnected. Total order clients: %d", len(s.orderWsClients))
		s.orderClientsMutex.Unlock()
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if messageType == websocket.TextMessage {
			// Send command to order processing channel
			log.Printf("Received order command: %q", message)
			select {
			case s.orderCommandChan <- string(message):
				// Successfully sent to channel
			default:
				log.Printf("Order command channel full, dropping message: %q", message)
			}
		}
	}
}

func (s *FuturesScanner) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	log.Printf("WebSocket connection attempt from %s", r.RemoteAddr)

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error from %s: %v", r.RemoteAddr, err)
		return
	}
	defer conn.Close()

	s.clientsMutex.Lock()
	s.wsClients[conn] = true
	clientCount := len(s.wsClients)
	s.clientsMutex.Unlock()

	log.Printf("WebSocket client connected from %s. Total clients: %d", r.RemoteAddr, clientCount)

	defer func() {
		s.clientsMutex.Lock()
		delete(s.wsClients, conn)
		log.Printf("WebSocket client disconnected. Total clients: %d", len(s.wsClients))
		s.clientsMutex.Unlock()
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		if messageType == websocket.TextMessage {
			s.handleClientMessage(string(message), conn)
		}
	}
}

func (s *FuturesScanner) handleClientMessage(message string, conn *websocket.Conn) {
	// Main WebSocket - only log commands that come here by mistake
	log.Printf("Unexpected command received on main WebSocket (should go to /ws/orders): %q", message)
}

func (s *FuturesScanner) executeArbitrageOrders(opportunityId string) {
	log.Printf("Received execute_arbitrage request for opportunity: %s", opportunityId)

	s.oppMutex.RLock()
	opportunity, exists := s.opportunities[opportunityId]
	s.oppMutex.RUnlock()

	if !exists {
		log.Printf("Opportunity %s not found in map with %d opportunities", opportunityId, len(s.opportunities))
		return
	}

	log.Printf("Found opportunity %s, creating orders", opportunityId)

	now := time.Now()

	// Create two orders: one buy, one sell
	buyOrder := ActiveOrder{
		Id:          fmt.Sprintf("buy_%s_%d", opportunityId, now.Unix()),
		Symbol:      opportunity.Symbol,
		Side:        "buy",
		Source:      opportunity.BuySource,
		Price:       opportunity.BuyPrice,
		Quantity:    0.001, // In real implementation, calculate based on available balance
		Status:      "pending",
		Timestamp:   now.UnixMilli(),
		Opportunity: &opportunity,
	}

	sellOrder := ActiveOrder{
		Id:          fmt.Sprintf("sell_%s_%d", opportunityId, now.Unix()),
		Symbol:      opportunity.Symbol,
		Side:        "sell",
		Source:      opportunity.SellSource,
		Price:       opportunity.SellPrice,
		Quantity:    0.001,
		Status:      "pending",
		Timestamp:   now.UnixMilli(),
		Opportunity: &opportunity,
	}

	s.ordersMutex.Lock()
	s.activeOrders = append(s.activeOrders, buyOrder, sellOrder)
	s.ordersMutex.Unlock()

	// Broadcast the new orders
	s.broadcastActiveOrders()
}

func (s *FuturesScanner) closeOrder(orderId string) {
	s.ordersMutex.Lock()

	var closedOpportunityId string
	// Find and update the order status
	for i, order := range s.activeOrders {
		if order.Id == orderId {
			s.activeOrders[i].Status = "closed"
			if order.Opportunity != nil {
				closedOpportunityId = order.Opportunity.Id
			}
			log.Printf("Closed order %s", orderId)
			break
		}
	}

	// Check if all orders with the same opportunityId are now closed
	if closedOpportunityId != "" {
		allClosed := true
		for _, order := range s.activeOrders {
			if order.Opportunity != nil && order.Opportunity.Id == closedOpportunityId && order.Status != "closed" {
				allClosed = false
				break
			}
		}

		// If all orders for this opportunity are closed, remove them
		if allClosed {
			var filteredOrders []ActiveOrder
			for _, order := range s.activeOrders {
				if order.Opportunity == nil || order.Opportunity.Id != closedOpportunityId {
					filteredOrders = append(filteredOrders, order)
				}
			}
			s.activeOrders = filteredOrders
			log.Printf("All orders for opportunity %s are closed, removed from active orders. Remaining orders: %d", closedOpportunityId, len(s.activeOrders))
		}
	}

	s.ordersMutex.Unlock() // release before broadcasting

	log.Printf("About to broadcast active orders after close, total orders: %d", len(s.activeOrders))

	// Broadcast the updated orders
	s.broadcastActiveOrders()
}

func (s *FuturesScanner) clearClosedOpportunities() {
	s.ordersMutex.Lock()
	defer s.ordersMutex.Unlock()

	// Group orders by opportunity ID
	ordersByOpportunity := make(map[string][]ActiveOrder)
	for _, order := range s.activeOrders {
		if order.Opportunity != nil {
			oppId := order.Opportunity.Id
			ordersByOpportunity[oppId] = append(ordersByOpportunity[oppId], order)
		}
	}

	// Find opportunities where all orders are closed
	var opportunitiesToRemove []string
	for oppId, orders := range ordersByOpportunity {
		allClosed := true
		for _, order := range orders {
			if order.Status != "closed" {
				allClosed = false
				break
			}
		}
		if allClosed {
			opportunitiesToRemove = append(opportunitiesToRemove, oppId)
		}
	}

	// Remove all orders for closed opportunities
	var filteredOrders []ActiveOrder
	for _, order := range s.activeOrders {
		if order.Opportunity != nil {
			shouldRemove := false
			for _, oppId := range opportunitiesToRemove {
				if order.Opportunity.Id == oppId {
					shouldRemove = true
					break
				}
			}
			if !shouldRemove {
				filteredOrders = append(filteredOrders, order)
			}
		} else {
			// Keep orders without opportunities
			filteredOrders = append(filteredOrders, order)
		}
	}

	s.activeOrders = filteredOrders
	log.Printf("Cleared %d closed opportunities, remaining orders: %d", len(opportunitiesToRemove), len(s.activeOrders))

	// Broadcast the updated orders
	go s.broadcastActiveOrders()
}

func (s *FuturesScanner) broadcastActiveOrders() {
	s.ordersMutex.RLock()
	ordersCopy := make([]ActiveOrder, len(s.activeOrders))
	copy(ordersCopy, s.activeOrders)
	s.ordersMutex.RUnlock()

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("EXCEPTION broadcasting", r)
		}
	}()

	log.Printf("Copied %d orders to broadcast to orders clients", len(ordersCopy))

	message := map[string]interface{}{
		"type":   "active_orders",
		"orders": ordersCopy,
	}

	log.Printf("Trying to lock order clients ws...")
	s.orderClientsMutex.RLock()
	log.Printf("Locked order clients ws...")
	clients := make([]*websocket.Conn, 0, len(s.orderWsClients))
	for client := range s.orderWsClients {
		clients = append(clients, client)
	}
	s.orderClientsMutex.RUnlock()

	log.Printf("Broadcasting active_orders message to %d order clients", len(clients))

	if len(clients) == 0 {
		log.Printf("No order clients connected, skipping broadcast")
		return
	}

	s.wsWriteMutex.Lock()
	defer s.wsWriteMutex.Unlock()

	var toRemove []*websocket.Conn
	for _, client := range clients {
		b, _ := json.Marshal(message)
		// log.Printf("JSON to send: %s", string(b))
		err := client.WriteJSON(message)
		if err != nil {
			log.Printf("Orders WebSocket write error: %v", err)
			client.Close()
			toRemove = append(toRemove, client)
		} else {
			log.Printf("Successfully sent active_orders message to order client")
		}
	}

	// Remove failed clients
	if len(toRemove) > 0 {
		s.orderClientsMutex.Lock()
		for _, client := range toRemove {
			delete(s.orderWsClients, client)
		}
		s.orderClientsMutex.Unlock()
		log.Printf("Removed %d failed order WebSocket clients", len(toRemove))
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using system environment variables")
	}

	scanner := NewFuturesScanner()

	symbols := []string{"BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "MYXUSDT", "0GUSDT"}

	// Start processing goroutines
	go scanner.processPrices()
	go scanner.processOrderbooks()
	go scanner.processTrades()
	go scanner.processOrderCommands()

	// Start exchange connections with orderbook feeds
	go exchanges.ConnectBinanceFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectBybitFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectHyperliquidFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectKrakenFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectOKXFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectGateFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectParadexFutures(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)

	// Start spot exchange connections with orderbook feeds
	go exchanges.ConnectBinanceSpot(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)
	go exchanges.ConnectBybitSpot(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)

	// Start Pyth price feed connection
	go exchanges.ConnectPythPrices(symbols, scanner.priceChan, scanner.orderbookChan, scanner.tradeChan)

	go scanner.broadcastPrices()

	http.HandleFunc("/ws", scanner.handleWebSocket)
	http.HandleFunc("/ws/orders", scanner.handleOrdersWebSocket)
	http.Handle("/", http.FileServer(http.Dir("./static/")))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	log.Printf("Server starting on http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
