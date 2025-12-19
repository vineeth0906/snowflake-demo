# Snowflake Medallion Pipeline Demo

Automated data pipeline: Raw → Curated → Publish layers using Git + Snowflake.

## 📁 Files
```
project/
├── .github/workflows/pipeline.yml   # Auto-runs on push
├── generate_data.py                 # Creates CSV files
├── deploy.py                        # Creates tables
├── load_raw.py                      # Loads to raw layer
├── transform.py                     # Transforms data
├── config.yaml                      # Your Snowflake details
├── requirements.txt                 # Python packages
└── README.md                        # This file
```

## 🚀 Setup (5 minutes)

### 1. Update config.yaml
```yaml
snowflake:
  account: "abc12345.us-east-1"  # From your Snowflake URL
  # Do NOT store secrets here. Use environment variables or a .env file instead.
  user: "YOUR_USERNAME"
  password: "YOUR_PASSWORD"
```

### 1.1 (Recommended) Create a `.env` file
Create a `.env` at the repo root with your credentials (gitignore it):

```text
# .env (example - DO NOT commit)
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
```

Scripts will load `.env` automatically (via `python-dotenv`) if present.

### 1.2 Use the sample file
Copy the included sample and fill it in, then keep the real `.env` out of source control:

```powershell
copy .env.sample .env
# (on mac/linux) cp .env.sample .env
```

`.env` is already included in `.gitignore` so credentials won't be committed.

### 2. Test Locally (Optional)
```bash
pip install -r requirements.txt
python generate_data.py
python deploy.py
python load_raw.py
python transform.py
```

### 3. Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin YOUR_REPO_URL
git push -u origin main
```

### 4. Add GitHub Secrets
Settings → Secrets → Actions → Add:
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`

**Done!** The pipeline runs automatically.

## 📊 What Gets Created

### Databases
- `RAW_DB` - Bronze/Raw layer
- `CURATED_DB` - Silver/Curated layer  
- `PUBLISH_DB` - Gold/Analytics layer

### Tables Created
| Layer | Table | Description |
|-------|-------|-------------|
| Raw | `customers_raw` | 100 customer records |
| Raw | `orders_raw` | 500 order records |
| Curated | `customers` | Cleaned (full names, country codes) |
| Curated | `orders` | With tax & totals calculated |
| Publish | `customer_summary` | Aggregated by customer |
| Publish | `daily_sales` | Aggregated by date |

## ✅ Verify in Snowflake

```sql
-- Check Raw layer
USE RAW_DB.STAGE;
SELECT COUNT(*) FROM customers_raw;
SELECT COUNT(*) FROM orders_raw;

-- Check Curated layer
USE CURATED_DB.DATA;
SELECT * FROM customers LIMIT 5;
SELECT * FROM orders LIMIT 5;

-- Check Publish layer
USE PUBLISH_DB.ANALYTICS;
SELECT * FROM customer_summary ORDER BY total_spent DESC LIMIT 10;
SELECT * FROM daily_sales ORDER BY order_date DESC LIMIT 10;
```

## 🎯 Demo Points

1. **Show Architecture**: Raw → Curated → Publish (3 layers)
2. **Show Automation**: Push code → GitHub Actions runs automatically
3. **Show Transformations**:
   - Raw: Original data
   - Curated: Combined names, country codes, tax calculations
   - Publish: Customer segments (VIP/Regular/New), daily totals
4. **Show Results**: Query each layer in Snowflake

## 🔄 Re-run Pipeline

**Manual:**
```bash
python generate_data.py && python load_raw.py && python transform.py
```

**Automatic:**
- Just push any change to GitHub
- Or click "Run workflow" in Actions tab

## ⚠️ Troubleshooting

**"Account not found"**
- Format: `account.region` (no https://)
- Example: `abc12345.us-east-1`

**"Warehouse not found"**
```sql
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE='XSMALL';
```

**GitHub Actions failing**
- Check secrets are named exactly: `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`
- Check Actions tab for error details

## 📞 Support
Check GitHub Actions logs for errors.