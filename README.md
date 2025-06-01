# Stablecoin Market Cap Data Pipeline

A robust Python pipeline that fetches **daily** historical market capitalization data for major stablecoins from the CoinGecko API and stores it in a PostgreSQL database hosted on Supabase.

**📅 Data Coverage**: Last 365 days (CoinGecko free tier limitation)

## 🚀 Features

- **Daily Data Collection**: Fetches the last 365 days of daily historical data for 6 major stablecoins
- **Free Tier Optimized**: Works within CoinGecko's free tier limitations (365 days, 30 calls/minute)
- **Rate Limit Compliance**: Respects CoinGecko's free tier limits (30 calls/minute)
- **Data Quality Validation**: Implements price range validation and anomaly detection
- **Robust Error Handling**: Continues processing other coins if one fails
- **Database Optimization**: Uses UPSERT operations and batch inserts for efficiency
- **Production Ready**: Comprehensive logging, retry logic, and transaction management

## 📊 Supported Stablecoins

| Coin        | Symbol | CoinGecko ID  |
| ----------- | ------ | ------------- |
| Tether      | USDT   | `tether`      |
| USD Coin    | USDC   | `usd-coin`    |
| Dai         | DAI    | `dai`         |
| Binance USD | BUSD   | `binance-usd` |
| Frax        | FRAX   | `frax`        |
| TrueUSD     | TUSD   | `true-usd`    |

## 🛠️ Setup Instructions

### 1. Clone and Install Dependencies

```bash
# Navigate to your project directory
cd your-project-directory

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Database Setup (Supabase)

1. **Create a Supabase Account**: Go to [supabase.com](https://supabase.com) and create a new project
2. **Get Database Credentials**: Navigate to Settings → Database and copy your connection details
3. **Note**: The pipeline will automatically create the required table and indexes

### 3. Environment Configuration

1. **Copy the example environment file**:

   ```bash
   cp env.example .env
   ```

2. **Edit `.env` with your credentials**:

   ```env
   # CoinGecko API Configuration (optional for free tier)
   COINGECKO_API_KEY=your_api_key_here_optional

   # Supabase PostgreSQL Database Configuration
   SUPABASE_DB_HOST=your_supabase_host
   SUPABASE_DB_NAME=postgres
   SUPABASE_DB_USER=postgres
   SUPABASE_DB_PASSWORD=your_password
   SUPABASE_DB_PORT=5432
   ```

   **Where to find your Supabase credentials**:

   - Go to your Supabase project dashboard
   - Navigate to Settings → Database
   - Copy the connection details under "Connection string"

### 4. CoinGecko API Key (Optional)

- **Free Tier**: No API key required (30 calls/minute, 365 days of data)
- **Paid Tier**: Add your API key to `.env` for higher rate limits and historical data access

## 🔄 Usage

### Run the Complete Pipeline

```bash
python stablecoin_data_pipeline.py
```

### Expected Output

```
2024-01-XX XX:XX:XX - INFO - 🚀 Starting Stablecoin Data Pipeline (Daily Data Collection - Last 365 Days)
2024-01-XX XX:XX:XX - INFO - ✅ Successfully connected to PostgreSQL database
2024-01-XX XX:XX:XX - INFO - ✅ Database table and indexes created/verified (daily data only)

📍 Processing daily data for tether (last 365 days)...
2024-01-XX XX:XX:XX - INFO - 📊 Fetching daily market data for tether (last 365 days)
2024-01-XX XX:XX:XX - INFO - ✅ Successfully fetched 365 daily data points for tether
2024-01-XX XX:XX:XX - INFO - 📈 Processing 365 daily records for tether
2024-01-XX XX:XX:XX - INFO - ✅ Processed 365 valid daily records for tether
2024-01-XX XX:XX:XX - INFO - ✅ Successfully upserted 365 records
2024-01-XX XX:XX:XX - INFO - ✅ Completed daily data processing for tether

... (continues for all stablecoins)

📊 DAILY DATA PIPELINE SUMMARY (LAST 365 DAYS)
✅ Successful: 6 coins
❌ Failed: 0 coins
🎉 Successfully processed daily data: tether, usd-coin, dai, binance-usd, frax, true-usd

📈 DAILY DATA STATISTICS (LAST 365 DAYS)
tether: 365 daily records (2024-01-XX to 2025-01-XX)
usd-coin: 365 daily records (2024-01-XX to 2025-01-XX)
dai: 365 daily records (2024-01-XX to 2025-01-XX)
binance-usd: 365 daily records (2024-01-XX to 2025-01-XX)
frax: 365 daily records (2024-01-XX to 2025-01-XX)
true-usd: 365 daily records (2024-01-XX to 2025-01-XX)
📊 Total daily records in database: 2,190
```

## 🗄️ Database Schema

The pipeline creates a table `stablecoin_market_caps` with the following structure:

```sql
CREATE TABLE stablecoin_market_caps (
    id BIGSERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,           -- CoinGecko coin ID
    coin_name VARCHAR(100) NOT NULL,        -- Human readable name
    coin_symbol VARCHAR(10) NOT NULL,       -- Token symbol (USDT, USDC, etc.)
    timestamp_utc TIMESTAMPTZ NOT NULL,     -- UTC timestamp (daily)
    market_cap_usd NUMERIC(20,2),          -- Market cap in USD
    price_usd NUMERIC(12,6),               -- Price in USD
    volume_24h_usd NUMERIC(20,2),          -- 24h volume in USD
    data_granularity VARCHAR(20) DEFAULT 'daily', -- Always 'daily'
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT unique_coin_timestamp UNIQUE(coin_id, timestamp_utc)
);
```

### Indexes Created

- `idx_coin_timestamp`: Composite index on (coin_id, timestamp_utc)
- `idx_timestamp`: Index on timestamp_utc for time-series queries
- `idx_coin_id`: Index on coin_id for filtering by coin

## 📈 Data Quality Features

### Price Validation

- Validates stablecoin prices are within $0.90 - $1.10 range
- Logs price anomalies for investigation
- Continues processing despite anomalies

### Data Integrity

- Uses `NUMERIC` data types to avoid floating-point precision issues
- Implements UPSERT logic to handle duplicate data
- Validates market cap values are positive

### Error Handling

- Continues processing other coins if one fails
- Implements exponential backoff for API retries
- Comprehensive logging for debugging

## ⏱️ Performance Considerations

### Rate Limiting

- **Free Tier**: 2.1 second delay between requests (30 calls/minute)
- **Data Limitation**: Free tier limited to last 365 days of historical data
- **Estimated Runtime**: ~2-3 minutes for 365-day data backfill of all 6 coins
- Automatic retry with exponential backoff for rate limits

### Database Optimization

- Batch inserts with 1,000 record pages
- Transaction management with rollback on errors
- Efficient indexing for daily time-series query performance
- Daily data granularity reduces storage requirements and improves query speed
- Smaller dataset (365 days vs years) means faster processing and queries

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Failed**

   ```
   ❌ Failed to connect to database: connection failed
   ```

   - Verify your Supabase credentials in `.env`
   - Check if your IP is allowed in Supabase settings
   - Ensure the database is running

2. **Rate Limited by CoinGecko**

   ```
   ⏳ Rate limited, waiting 60 seconds...
   ```

   - This is normal for free tier usage
   - The pipeline will automatically retry

3. **Missing Environment Variables**
   ```
   Missing required environment variables: SUPABASE_DB_HOST
   ```
   - Ensure all required variables are set in `.env`
   - Copy from `env.example` and fill in your values

### Debug Mode

To enable debug logging, modify the logging level in the script:

```python
logging.basicConfig(
    level=logging.DEBUG,  # Change from INFO to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
```

## 📊 Example Queries

### Get Latest Market Caps

```sql
SELECT
    coin_symbol,
    coin_name,
    market_cap_usd,
    price_usd,
    timestamp_utc
FROM stablecoin_market_caps
WHERE timestamp_utc = (
    SELECT MAX(timestamp_utc)
    FROM stablecoin_market_caps s2
    WHERE s2.coin_id = stablecoin_market_caps.coin_id
)
ORDER BY market_cap_usd DESC;
```

### Market Cap Trends Over Time

```sql
SELECT
    coin_symbol,
    DATE(timestamp_utc) as date,
    AVG(market_cap_usd) as avg_market_cap,
    AVG(price_usd) as avg_price
FROM stablecoin_market_caps
WHERE timestamp_utc >= NOW() - INTERVAL '1 year'
GROUP BY coin_symbol, DATE(timestamp_utc)
ORDER BY date DESC, avg_market_cap DESC;
```

### Price Anomaly Detection

```sql
SELECT
    coin_symbol,
    price_usd,
    timestamp_utc,
    market_cap_usd
FROM stablecoin_market_caps
WHERE price_usd < 0.95 OR price_usd > 1.05
ORDER BY timestamp_utc DESC;
```

## 🔄 Re-running the Pipeline

The pipeline is designed to be idempotent:

- **UPSERT Logic**: Safely handles duplicate data
- **Incremental Updates**: Only new data is added
- **Data Validation**: Maintains data quality on re-runs

To update with latest data, simply run the pipeline again:

```bash
python stablecoin_data_pipeline.py
```

## 📝 License

This project is open source and available under the MIT License.

---

**Need Help?**

- Check the console output for detailed error messages
- Ensure all environment variables are correctly set
- Verify your Supabase database is accessible
- For CoinGecko API issues, check their status page
