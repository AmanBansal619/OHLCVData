import os
import requests
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional, Dict, Any

class StockDataFetcher:
    def __init__(self):
        # Get Alpha Vantage API key from environment variable
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")

        self.base_url = "https://www.alphavantage.co/query"
        self.cache = {}

    def get_live_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch live price data for a symbol using Alpha Vantage"""
        try:
            params = {
                "function": "GLOBAL_QUOTE",
                "symbol": symbol,
                "apikey": self.api_key
            }

            print(f"🔍 Fetching live price for {symbol}...")
            response = requests.get(self.base_url, params=params, timeout=10)

            print(f"📡 API Response Status: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ HTTP Error for {symbol}: {response.status_code}")
                print(f"   Full Response: {response.text}")
                return None

            data = response.json()

            # Check for Alpha Vantage specific errors
            if "Error Message" in data:
                print(f"❌ Alpha Vantage Error for {symbol}: {data['Error Message']}")
                print(f"   Full Response: {data}")
                return None

            if "Note" in data:
                print(f"⚠️ Alpha Vantage Note for {symbol}: {data['Note']}")
                print(f"   Full Response: {data}")

            if "Global Quote" not in data or not data["Global Quote"]:
                print(f"❌ No quote data available for {symbol}")
                print(f"   Response keys: {list(data.keys())}")
                print(f"   Full Response: {data}")
                return None

            quote = data["Global Quote"]
            print(f"📊 Quote data keys: {list(quote.keys())}")

            # Extract data from Alpha Vantage response
            current_price = float(quote.get("05. price", 0))
            previous_close = float(quote.get("08. previous close", 0))
            volume = int(quote.get("06. volume", 0))

            if current_price == 0:
                print(f"❌ Invalid price data for {symbol}: price = {current_price}")
                return None

            if current_price and previous_close:
                change_percent = ((current_price - previous_close) / previous_close) * 100
            else:
                change_percent = 0.0

            print(f"✅ Successfully fetched data for {symbol}: ₹{current_price:.2f}")
            return {
                'symbol': symbol,
                'price': current_price,
                'change_percent': change_percent,
                'volume': volume,
                'last_updated': datetime.now()
            }
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout error fetching {symbol}: Request timed out after 10 seconds")
            return None
        except requests.exceptions.ConnectionError:
            print(f"🌐 Connection error fetching {symbol}: Unable to connect to Alpha Vantage")
            return None
        except requests.exceptions.RequestException as e:
            print(f"📡 Request error fetching {symbol}: {e}")
            return None
        except ValueError as e:
            print(f"🔢 Data parsing error for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"💥 Unexpected error fetching {symbol}: {type(e).__name__}: {e}")
            return None

    def get_historic_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Fetch historic data for a symbol using Alpha Vantage"""
        try:
            # Convert period to Alpha Vantage format
            if period == "1y":
                function = "TIME_SERIES_DAILY"
                outputsize = "compact"
            elif period == "2y":
                function = "TIME_SERIES_DAILY"
                outputsize = "full"
            else:
                function = "TIME_SERIES_DAILY"
                outputsize = "compact"

            params = {
                "function": function,
                "symbol": symbol,
                "outputsize": outputsize,
                "apikey": self.api_key
            }

            print(f"📈 Fetching historical data for {symbol} ({period})...")
            response = requests.get(self.base_url, params=params, timeout=15)

            print(f"📡 Historical API Response Status: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ HTTP Error for {symbol} historical data: {response.status_code}")
                print(f"   Full Response: {response.text}")
                return None

            data = response.json()

            # Check for Alpha Vantage specific errors
            if "Error Message" in data:
                print(f"❌ Alpha Vantage Error for {symbol} historical: {data['Error Message']}")
                print(f"   Full Response: {data}")
                return None

            if "Note" in data:
                print(f"⚠️ Alpha Vantage Note for {symbol} historical: {data['Note']}")
                print(f"   Full Response: {data}")

            if "Time Series (Daily)" not in data:
                print(f"❌ No historical data available for {symbol}")
                print(f"   Response keys: {list(data.keys())}")
                print(f"   Full Response: {data}")
                return None

            time_series = data["Time Series (Daily)"]
            record_count = len(time_series)
            print(f"📊 Retrieved {record_count} historical records for {symbol}")

            # Convert to DataFrame
            records = []
            for date_str, daily_data in time_series.items():
                try:
                    records.append({
                        'Date': date_str,
                        'Open': float(daily_data['1. open']),
                        'High': float(daily_data['2. high']),
                        'Low': float(daily_data['3. low']),
                        'Close': float(daily_data['4. close']),
                        'Volume': int(daily_data['5. volume'])
                    })
                except (ValueError, KeyError) as e:
                    print(f"⚠️ Skipping invalid record for {date_str}: {e}")
                    continue

            if not records:
                print(f"❌ No valid records found for {symbol}")
                return None

            df = pd.DataFrame(records)
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            df.sort_index(inplace=True)

            print(f"✅ Successfully processed {len(df)} historical records for {symbol}")
            return df

        except requests.exceptions.Timeout:
            print(f"⏰ Timeout error fetching {symbol} historical data: Request timed out after 15 seconds")
            return None
        except requests.exceptions.ConnectionError:
            print(f"🌐 Connection error fetching {symbol} historical data: Unable to connect to Alpha Vantage")
            return None
        except requests.exceptions.RequestException as e:
            print(f"📡 Request error fetching {symbol} historical data: {e}")
            return None
        except ValueError as e:
            print(f"🔢 Data parsing error for {symbol} historical data: {e}")
            return None
        except Exception as e:
            print(f"💥 Unexpected error fetching {symbol} historical data: {type(e).__name__}: {e}")
            return None

    def get_historic_data_date_range(self, symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch historic data for a specific date range using Alpha Vantage"""
        try:
            # Alpha Vantage doesn't support custom date ranges directly
            # We'll fetch full data and filter
            df = self.get_historic_data(symbol, "2y")  # Get full 2 years

            if df is None or df.empty:
                return None

            # Filter by date range
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)

            mask = (df.index >= start) & (df.index <= end)
            filtered_df = df[mask]

            return filtered_df
        except Exception as e:
            print(f"Error fetching historic data for {symbol} in date range: {e}")
            return None

    def search_symbol(self, keywords: str) -> Optional[str]:
        """Search for the correct symbol using Alpha Vantage SYMBOL_SEARCH API"""
        try:
            params = {
                "function": "SYMBOL_SEARCH",
                "keywords": keywords,
                "apikey": self.api_key
            }

            print(f"🔍 Searching for symbol: {keywords}")
            response = requests.get(self.base_url, params=params, timeout=10)

            print(f"📡 Symbol Search Response Status: {response.status_code}")

            if response.status_code != 200:
                print(f"❌ HTTP Error in symbol search: {response.status_code}")
                print(f"   Full Response: {response.text}")
                return None

            data = response.json()

            # Check for Alpha Vantage specific errors
            if "Error Message" in data:
                print(f"❌ Alpha Vantage Error in symbol search: {data['Error Message']}")
                print(f"   Full Response: {data}")
                return None

            if "Note" in data:
                print(f"⚠️ Alpha Vantage Note in symbol search: {data['Note']}")

            # Check if we have search results
            if "bestMatches" not in data or not data["bestMatches"]:
                print(f"❌ No symbol matches found for: {keywords}")
                print(f"   Response keys: {list(data.keys())}")
                return None

            best_matches = data["bestMatches"]
            print(f"📊 Found {len(best_matches)} symbol matches")

            # Look for the best match (prioritize NSE symbols for Indian stocks)
            for match in best_matches:
                symbol = match.get("1. symbol", "")
                name = match.get("2. name", "")
                region = match.get("4. region", "")

                print(f"   Match: {symbol} - {name} ({region})")

                # For Indian stocks, prefer NSE (.NS) over BSE (.BO)
                if keywords.upper() in ["TCS", "RELIANCE", "INFY", "HDFCBANK", "ICICIBANK"]:
                    if symbol.endswith(".NS"):
                        print(f"✅ Selected NSE symbol: {symbol}")
                        return symbol
                    elif symbol.endswith(".BO") and not any(m.get("1. symbol", "").endswith(".NS") for m in best_matches):
                        print(f"✅ Selected BSE symbol: {symbol}")
                        return symbol

            # If no preference, return the first match
            if best_matches:
                best_symbol = best_matches[0].get("1. symbol", "")
                print(f"✅ Selected best match: {best_symbol}")
                return best_symbol

            return None

        except requests.exceptions.Timeout:
            print(f"⏰ Timeout error in symbol search for {keywords}: Request timed out after 10 seconds")
            return None
        except requests.exceptions.ConnectionError:
            print(f"🌐 Connection error in symbol search for {keywords}: Unable to connect to Alpha Vantage")
            return None
        except requests.exceptions.RequestException as e:
            print(f"📡 Request error in symbol search for {keywords}: {e}")
            return None
        except Exception as e:
            print(f"💥 Unexpected error in symbol search for {keywords}: {type(e).__name__}: {e}")
            return None

    def is_market_open(self) -> bool:
        """Check if the market is currently open (for informational purposes)"""
        now = datetime.now()
        # Simple check for weekdays and typical market hours (9:15 AM - 3:30 PM IST)
        # Note: Even when market is closed, Alpha Vantage provides latest available data
        if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_open <= now <= market_close
