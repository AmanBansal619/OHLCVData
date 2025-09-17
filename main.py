from fastapi import FastAPI, HTTPException, BackgroundTasks, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import uvicorn
from datetime import datetime
import asyncio
from dotenv import load_dotenv
from database import StockDatabase
from data_fetcher import StockDataFetcher


# Load environment variables
load_dotenv()

from database import StockDatabase
from data_fetcher import StockDataFetcher

app = FastAPI(title="Stock Market API", description="API for fetching stock prices and historic data", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
db = StockDatabase()
fetcher = StockDataFetcher()


# ----------------------------
# 📡 WebSocket Manager
# ----------------------------
class ConnectionManager:
    def __init__(self):
        # key = symbol, value = list of websocket connections
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, symbol: str):
        await websocket.accept()
        if symbol not in self.active_connections:
            self.active_connections[symbol] = []
        self.active_connections[symbol].append(websocket)
        print(f"🔌 Client connected for {symbol} | Total: {len(self.active_connections[symbol])}")

    def disconnect(self, websocket: WebSocket, symbol: str):
        if symbol in self.active_connections:
            self.active_connections[symbol].remove(websocket)
            if not self.active_connections[symbol]:
                del self.active_connections[symbol]
        print(f"❌ Client disconnected from {symbol}")

    async def broadcast(self, symbol: str, message: dict):
        if symbol in self.active_connections:
            for connection in list(self.active_connections[symbol]):
                try:
                    await connection.send_json(message)
                except Exception as e:
                    print(f"⚠️ Error sending to client ({symbol}): {e}")
                    self.disconnect(connection, symbol)


manager = ConnectionManager()

# ----------------------------
# 📡 WebSocket Endpoint
# ----------------------------
@app.websocket("/ws/stocks/{symbol}")

async def websocket_stock_data(websocket: WebSocket, symbol: str):
    await manager.connect(websocket, symbol)

    try:
        while True:
            # Fetch latest live price
            live_data = fetcher.get_live_price(symbol)

            if live_data:
                payload = {
                    "symbol": symbol,
                    "price": live_data["price"],
                    "change_percent": live_data["change_percent"],
                    "volume": live_data["volume"],
                    "last_updated": live_data["last_updated"].isoformat()
                }
                await manager.broadcast(symbol, payload)
            else:
                await manager.broadcast(symbol, {"error": f"No data for {symbol}"})

            # send every 10 seconds (tweak as needed)
            await asyncio.sleep(10)

    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)@app.websocket("/ws/stocks/{symbol}")
async def websocket_stock_data(websocket: WebSocket, symbol: str):
    await manager.connect(websocket, symbol)

    try:
        while True:
            # Fetch latest live price
            live_data = fetcher.get_live_price(symbol)

            if live_data:
                payload = {
                    "symbol": symbol,
                    "price": live_data["price"],
                    "change_percent": live_data["change_percent"],
                    "volume": live_data["volume"],
                    "last_updated": live_data["last_updated"].isoformat()
                }
                await manager.broadcast(symbol, payload)
            else:
                await manager.broadcast(symbol, {"error": f"No data for {symbol}"})

            # send every 10 seconds (tweak as needed)
            await asyncio.sleep(10)

    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)


@app.websocket("/ws/stocks/{symbol}/history")
async def websocket_stock_history(websocket: WebSocket, symbol: str):
    await manager.connect(websocket, symbol)

    try:
        # Use the same logic as your REST route without calling TestClient
        df = db.get_historic_data(symbol)
        if df.empty:
            historic_data = fetcher.get_historic_data(symbol)
            if historic_data is None or historic_data.empty:
                corrected_symbol = fetcher.search_symbol(symbol)
                if corrected_symbol and corrected_symbol != symbol:
                    historic_data = fetcher.get_historic_data(corrected_symbol)
                    symbol = corrected_symbol
            if historic_data is not None and not historic_data.empty:
                db.insert_historic_data(symbol, historic_data)
                df = db.get_historic_data(symbol)

        if df.empty:
            await websocket.send_json({"error": f"No historical data for {symbol}"})
            return

        # Stream the historical data
        for index, row in df.iloc[::-1].iterrows():
            timestamp = index.isoformat() if hasattr(index, "isoformat") else str(index)
            payload = {
                "symbol": symbol,
                "timestamp": timestamp,
                "open": float(row["open_price"]),
                "high": float(row["high_price"]),
                "low": float(row["low_price"]),
                "close": float(row["close_price"]),
                "volume": int(row["volume"])
            }
            await manager.broadcast(symbol, payload)
            await asyncio.sleep(0.5)

        await manager.broadcast(symbol, {"info": "history_end"})

        # Keep connection alive
        while True:
            await asyncio.sleep(30)

    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)
    except Exception as e:
        print(f"⚠️ WebSocket error for {symbol}: {e}")
        try:
            await websocket.send_json({"error": f"server_error: {str(e)}"})
        except Exception:
            pass
        manager.disconnect(websocket, symbol)


# Pydantic models
class StockQuery(BaseModel):
    symbol: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class StockResponse(BaseModel):
    symbol: str
    price: Optional[float]
    change_percent: Optional[float]
    volume: Optional[int]
    last_updated: Optional[datetime]

class HistoricDataResponse(BaseModel):
    symbol: str
    data: List[dict]

# Background task for updating popular stock prices
async def update_popular_stocks():
    """Background task to update prices for popular stocks with logging and backfill"""
    # Using symbols that are more likely to be available on Alpha Vantage
    popular_symbols = ["IBM", "AAPL", "MSFT", "GOOGL"]

    print(f"🚀 Starting background task for {len(popular_symbols)} popular stocks: {', '.join(popular_symbols)}")

    while True:
        cycle_start = datetime.now()
        print(f"📊 Starting update cycle at {cycle_start.strftime('%H:%M:%S')}")

        updated_count = 0
        backfill_count = 0

        try:
            for symbol in popular_symbols:
                try:
                    # Check if we need backfill (no data for today)
                    today = datetime.now().date()
                    existing_data = db.get_historic_data(symbol)

                    needs_backfill = False
                    if existing_data is None or existing_data.empty:
                        needs_backfill = True
                        print(f"📈 {symbol}: No historical data found, will backfill")
                    else:
                        try:
                            # Check if we have today's data
                            if len(existing_data) > 0 and hasattr(existing_data.index, 'max'):
                                latest_timestamp = existing_data.index.max()
                                if hasattr(latest_timestamp, 'date'):
                                    latest_date = latest_timestamp.date()
                                    if latest_date != today:
                                        needs_backfill = True
                                        print(f"📈 {symbol}: Missing today's data (latest: {latest_date}), will backfill")
                                else:
                                    # Handle case where index contains non-datetime values
                                    needs_backfill = True
                                    print(f"📈 {symbol}: Index format issue, will backfill")
                            else:
                                needs_backfill = True
                                print(f"📈 {symbol}: Empty or invalid data, will backfill")
                        except Exception as date_error:
                            print(f"⚠️ {symbol}: Error checking dates ({date_error}), will backfill")
                            needs_backfill = True

                    # Always try to fetch latest price data
                    live_data = fetcher.get_live_price(symbol)
                    if live_data:
                        db.update_latest_price(
                            live_data['symbol'],
                            live_data['price'],
                            live_data['change_percent'],
                            live_data['volume']
                        )
                        print(f"✅ {symbol}: Updated price to ₹{live_data['price']:.2f} ({live_data['change_percent']:+.2f}%)")
                        updated_count += 1
                    else:
                        print(f"❌ {symbol}: Failed to fetch live price")

                    # Backfill historical data if needed
                    if needs_backfill:
                        try:
                            # Fetch last 7 days to ensure we have recent data
                            recent_data = fetcher.get_historic_data(symbol, "1y")  # Get 1 year of data
                            if recent_data is not None and not recent_data.empty:
                                db.insert_historic_data(symbol, recent_data)
                                new_records = len(recent_data)
                                print(f"📊 {symbol}: Backfilled {new_records} historical records")
                                backfill_count += 1
                            else:
                                print(f"⚠️  {symbol}: No historical data available for backfill")
                        except Exception as backfill_error:
                            print(f"❌ {symbol}: Backfill failed - {backfill_error}")

                except Exception as symbol_error:
                    print(f"❌ {symbol}: Error processing - {symbol_error}")

                # Small delay between symbols to respect API rate limits
                await asyncio.sleep(1)

            cycle_end = datetime.now()
            duration = (cycle_end - cycle_start).total_seconds()

            print(f"📊 Cycle completed in {duration:.1f}s")
            print(f"✅ Updated {updated_count}/{len(popular_symbols)} prices")
            print(f"📈 Backfilled {backfill_count}/{len(popular_symbols)} symbols")
            print(f"⏰ Next cycle in 5 minutes...")

        except Exception as e:
            print(f"💥 Critical error in update cycle: {e}")

        # Wait for 5 minutes before next update cycle
        await asyncio.sleep(300)

@app.on_event("startup")
async def startup_event():
    """Initialize the database with historic data on startup"""
    print("🚀 Starting Stock Market API...")
    print("📊 Data Source: Alpha Vantage API")
    print("🗄️  Database: stock_data.duckdb")

    try:
        # Load historic data for IBM (reliable test symbol)
        print("📈 Loading initial historical data for IBM...")
        historic_data = fetcher.get_historic_data("IBM", period="2y")
        if historic_data is not None:
            db.insert_historic_data("IBM", historic_data)
            records_count = len(historic_data)
            print(f"✅ Loaded {records_count} historical records for IBM")
        else:
            print("⚠️  Failed to load initial IBM data (will be fetched on-demand)")

        print("🔄 Starting background task for popular stocks...")
        # Start background task for popular stock updates
        asyncio.create_task(update_popular_stocks())

        print("🎯 API ready! Endpoints available:")
        print("   GET  /                    - API info")
        print("   GET  /stocks/{symbol}     - Latest price")
        print("   GET  /stocks/{symbol}/history - Historical data")
        print("   GET  /market/status       - Market status")
        print("   GET  /system/status       - System status")
        print("   GET  /stocks              - Available stocks")

    except Exception as e:
        print(f"💥 Error during startup: {e}")
        print("⚠️  API may still work but background tasks might be limited")

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Stock Market API", "version": "1.0.0"}

@app.get("/stocks/{symbol}", response_model=StockResponse)
async def get_stock_price(symbol: str):
    """Get the latest price for a stock symbol"""
    try:
        # Try to get from database first
        db_result = db.get_latest_price(symbol)
        if db_result:
            return StockResponse(**db_result)

        # If not in DB, fetch live data
        live_data = fetcher.get_live_price(symbol)
        if live_data:
            # Update database
            db.update_latest_price(
                live_data['symbol'],
                live_data['price'],
                live_data['change_percent'],
                live_data['volume']
            )
            return StockResponse(**live_data)

        # If direct fetch failed, try symbol search for common mistakes
        print(f"⚠️ Direct fetch failed for {symbol}, attempting symbol search...")
        corrected_symbol = fetcher.search_symbol(symbol)

        if corrected_symbol and corrected_symbol != symbol:
            print(f"🔄 Found corrected symbol: {corrected_symbol}")
            # Try again with corrected symbol
            live_data = fetcher.get_live_price(corrected_symbol)
            if live_data:
                # Update database with corrected symbol
                db.update_latest_price(
                    live_data['symbol'],
                    live_data['price'],
                    live_data['change_percent'],
                    live_data['volume']
                )
                return StockResponse(**live_data)

        # If still no data, provide helpful error message
        error_msg = f"Stock data not found for symbol: {symbol}"
        if corrected_symbol and corrected_symbol != symbol:
            error_msg += f". Did you mean: {corrected_symbol}?"
        elif not corrected_symbol:
            error_msg += ". Please check the symbol format (e.g., TCS.NS for NSE stocks)"

        raise HTTPException(status_code=404, detail=error_msg)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stock data: {str(e)}")

@app.get("/stocks/{symbol}/history", response_model=HistoricDataResponse)
async def get_stock_history(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    """Get historic data for a stock symbol"""
    try:
        # Get data from database
        df = db.get_historic_data(symbol, start_date, end_date)

        if df.empty:
            # If no data in DB, try to fetch and store
            if start_date and end_date:
                historic_data = fetcher.get_historic_data_date_range(symbol, start_date, end_date)
            else:
                historic_data = fetcher.get_historic_data(symbol)

            # If direct fetch failed, try symbol search
            if historic_data is None or historic_data.empty:
                print(f"⚠️ Direct historical fetch failed for {symbol}, attempting symbol search...")
                corrected_symbol = fetcher.search_symbol(symbol)

                if corrected_symbol and corrected_symbol != symbol:
                    print(f"🔄 Found corrected symbol for history: {corrected_symbol}")
                    # Try again with corrected symbol
                    if start_date and end_date:
                        historic_data = fetcher.get_historic_data_date_range(corrected_symbol, start_date, end_date)
                    else:
                        historic_data = fetcher.get_historic_data(corrected_symbol)

                    # Update symbol for database operations
                    if historic_data is not None and not historic_data.empty:
                        symbol = corrected_symbol  # Use corrected symbol for DB operations

            if historic_data is not None and not historic_data.empty:
                db.insert_historic_data(symbol, historic_data)
                df = db.get_historic_data(symbol, start_date, end_date)

        # Convert to list of dicts
        data_list = []
        for index, row in df.iterrows():
            try:
                # Handle different index formats safely
                if hasattr(index, 'isoformat'):
                    timestamp = index.isoformat()
                elif hasattr(index, 'strftime'):
                    timestamp = index.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    # Fallback for unexpected formats
                    timestamp = str(index)

                data_list.append({
                    'timestamp': timestamp,
                    'open': float(row['open_price']),
                    'high': float(row['high_price']),
                    'low': float(row['low_price']),
                    'close': float(row['close_price']),
                    'volume': int(row['volume'])
                })
            except Exception as row_error:
                print(f"⚠️ Skipping invalid row: {row_error}")
                continue

        return HistoricDataResponse(symbol=symbol, data=data_list)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching historic data: {str(e)}")

@app.get("/market/status")
async def get_market_status():
    """Get current market status"""
    is_open = fetcher.is_market_open()
    return {
        "market_open": is_open,
        "data_available": True,  # Alpha Vantage always provides latest available data
        "current_time": datetime.now().isoformat(),
        "timezone": "IST",
        "note": "Latest available data can be fetched anytime, even when market is closed"
    }

@app.get("/stocks")
async def get_available_stocks():
    """Get list of available stocks"""
    symbols = db.get_all_symbols()
    return {"available_stocks": symbols}

@app.get("/system/status")
async def get_system_status():
    """Get system status including background task info"""
    popular_symbols = ["IBM", "AAPL", "MSFT", "GOOGL"]

    status_info = {
        "database_path": "stock_data.duckdb",
        "data_source": "Alpha Vantage API",
        "popular_symbols_tracked": popular_symbols,
        "background_task": {
            "active": True,
            "update_interval_minutes": 5,
            "symbols_per_cycle": len(popular_symbols)
        },
        "api_limits": {
            "alpha_vantage_free_tier": "25 calls/day, 5 calls/minute"
        }
    }

    # Check data freshness for popular symbols
    data_status = {}
    for symbol in popular_symbols:
        try:
            latest_price = db.get_latest_price(symbol)
            historic_data = db.get_historic_data(symbol)

            if latest_price:
                last_update = latest_price['last_updated']
                hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
                data_status[symbol] = {
                    "price": latest_price['price'],
                    "last_updated_hours_ago": round(hours_since_update, 1),
                    "historic_records": len(historic_data) if historic_data is not None else 0
                }
            else:
                data_status[symbol] = {"status": "no_data"}
        except Exception as e:
            data_status[symbol] = {"error": str(e)}

    status_info["data_status"] = data_status
    return status_info

@app.get("/debug/api-test")
async def test_api_connection():
    """Test Alpha Vantage API connection and key validity"""
    try:
        # Test with a simple symbol
        test_symbol = "IBM"  # International symbol that's usually available

        print("🔧 Running API connectivity test...")

        # Test live price
        live_result = fetcher.get_live_price(test_symbol)

        # Test historical data
        hist_result = fetcher.get_historic_data(test_symbol, "compact")

        test_results = {
            "api_key_configured": bool(fetcher.api_key),
            "api_key_length": len(fetcher.api_key) if fetcher.api_key else 0,
            "test_symbol": test_symbol,
            "live_price_test": "✅ Success" if live_result else "❌ Failed",
            "historical_data_test": "✅ Success" if hist_result is not None else "❌ Failed",
            "timestamp": datetime.now().isoformat()
        }

        if live_result:
            test_results["sample_price"] = live_result['price']

        if hist_result is not None:
            test_results["sample_records"] = len(hist_result)

        print(f"🔧 API Test Results: Live={test_results['live_price_test']}, Historical={test_results['historical_data_test']}")

        return test_results

    except Exception as e:
        return {
            "error": f"API test failed: {str(e)}",
            "api_key_configured": bool(fetcher.api_key),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
