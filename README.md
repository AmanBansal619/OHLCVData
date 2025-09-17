Here’s a polished and professional version of your instructions:

---

### **Setup and Usage Instructions**

1. **Create and activate a virtual environment**

   * This ensures dependencies are isolated from your system Python.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # macOS/Linux
   .venv\Scripts\activate      # Windows
   ```

2. **Obtain a free API key**

   * Go to [Alpha Vantage](https://www.alphavantage.co/) and sign up for a free API key.
   * This key allows **25 requests per day** for live stock data, which is sufficient for testing.
   * Historical data can be accessed without additional limits.

3. **Configure the `.env` file**

   * In your project directory, open the `.env` file.
   * Replace the placeholder with your generated API key:

   ```env
   ALPHA_VANTAGE_API_KEY=YOUR_GENERATED_KEY
   ```

4. **Install dependencies and start the server**

   ```bash
   pip install -r requirements.txt
   python main.py   # or the name of your Flask server script
   ```

5. **Access the frontend**

   * Open `a.html` in your browser.
   * The page connects to the running Flask server and displays live and historical stock data.

---

💡 **Notes:**

* **Live data**: Limited to 25 requests per API key per day.
* **Historical data**: Fully available without API limits.
* Ensure the server is running before opening the frontend HTML file.

---
