# ⚡ crypto arbitrage scanner

real-time crypto futures arbitrage scanner, built in go and plain javascript. connect to multiple exchanges right at the websocket layer. shows live price gaps and where the spread hides.

## what's this about?

market microstructure is the study of how prices form in split-up, messy markets. a single asset never has a single price. every exchange has its own order book, its own little quirks. so, the price drifts—sometimes by a lot, usually for just milliseconds.

in crypto, this isn't hidden behind expensive pro feeds. you can actually see the gaps yourself if you have the right tools. that's what this project does: surfaces live, structural arbitrage opportunities, so you can watch price discovery as it happens.

## what can it do?

- connect to 9 spot/futures exchanges (binance, bybit, hyperliquid, kraken, okx, gate.io, paradex) over websockets
- live arbitrage matrix: highlights when the price difference is big enough
- watch multiple pairs: btcusdt, ethusdt, xrpusdt, solusdt
- auto adjusts decimals by asset/price
- live tradingview lightweight charts
- spot and alert on inefficient price gaps, in real time

## how does it work?

- **backend (go):**
    - every exchange runs in its own goroutine, fetches orderbook data live via websockets
    - calculates mid-price using (best bid + best ask) / 2
    - all the data gets passed through go channels, no locks slowing things down
    - once prices land, calculates spreads & arbitrage. broadcasts over one websocket to all frontends

- **frontend:**
    - vanilla js
    - uses tradingview lightweight charts

## how to run

1. open a terminal, start the backend:

    ```
    go run main.go
    ```

2. open your browser. head over to `http://localhost:8082`

## pairs & exchanges

- btcusdt
- ethusdt
- xrpusdt
- solusdt

covers:

**futures exchanges:**
- binance futures
- bybit futures
- hyperliquid (dex) futures
- kraken futures
- okx futures
- gate.io futures
- paradex futures

**spot exchanges:**
- binance spot
- bybit spot

## config

set your own minimum spread for alerts in the ui (default is 0.05%, after estimated fees).
