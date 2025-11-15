# Getting Started - Complete Setup Guide

This guide will help you get the Twitter Climate Sentiment Pipeline running on your machine in **under 15 minutes**.

---

## 🎯 Quick Start (No API Keys Required)

If you just want to see it working immediately:

```bash
# 1. Install Python dependencies
pip install pandas pyarrow emoji contractions

# 2. Run with sample data
cd src
python topology.py --local
```

That's it! The pipeline will process the sample tweets and save results to `/tmp/twitter_climate/`.

---

## 📋 Prerequisites

- **Python 3.8+** (check with `python --version`)
- **Git** (for cloning the repository)
- **pip** (Python package manager)
- **Optional**: Docker Desktop (for Storm cluster)

---

## 🚀 Complete Setup (Step-by-Step)

### Step 1: Get the Code

#### Option A: Push to GitHub (if you have code locally)

```bash
# 1. Initialize git (if not already done)
git init

# 2. Add all files
git add .

# 3. Commit
git commit -m "Initial commit: Twitter Climate Sentiment Pipeline"

# 4. Create a new repository on GitHub
# Go to https://github.com/new
# Name it: twitter-climate-sentiment-pipeline
# Don't initialize with README (you already have one)

# 5. Push to GitHub
git remote add origin https://github.com/YOUR_USERNAME/twitter-climate-sentiment-pipeline.git
git branch -M main
git push -u origin main
```

#### Option B: If cloning from GitHub

```bash
git clone https://github.com/YOUR_USERNAME/twitter-climate-sentiment-pipeline.git
cd twitter-climate-sentiment-pipeline
```

---

### Step 2: Install Python Dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# This installs:
# - pandas, pyarrow (data processing)
# - emoji, contractions (text processing)
# - transformers, torch (sentiment models)
# - fasttext, langdetect (language detection)
# - geopy (geocoding)
# - requests, redis (APIs and caching)
# - folium, plotly (visualizations)
# - pyhive (Hive connectivity)
```

**Note**: This may take 5-10 minutes as it downloads ML libraries.

---

### Step 3: Download ML Models (Optional but Recommended)

```bash
python scripts/download_models.py
```

This downloads:
- fastText language identification model (~130MB)
- RoBERTa sentiment model (~500MB)
- XLM-RoBERTa multilingual model (~1GB)

**Skip this if**: You want to use VADER fallback (lighter, less accurate)

---

### Step 4: Configure Environment (Optional)

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your API credentials (optional)
# nano .env  # or use any text editor
```

**For Twitter API** (optional):
1. Go to https://developer.twitter.com/en/portal/dashboard
2. Create a new app
3. Get your Bearer Token
4. Add to `.env`: `TWITTER_BEARER_TOKEN=your_token_here`

**For NOAA API** (optional):
1. Go to https://www.ncdc.noaa.gov/cdo-web/token
2. Request a token (free)
3. Add to `.env`: `NOAA_API_TOKEN=your_token_here`

**Note**: You can run without API keys using sample data!

---

### Step 5: Run the Pipeline

#### Option A: Quick Test with Sample Data (Recommended First)

```bash
# Set environment for local mode
export USE_LOCAL_FILE=true
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate

# Run pipeline
python src/topology.py --local
```

You should see output like:
```
========================================
Twitter Climate Sentiment Pipeline - LOCAL MODE
========================================

Configuration:
  Twitter API: Not configured
  NOAA API: Not configured
  Use local file: true
  Storage: /tmp/twitter_climate

Initializing components...
✓ All components initialized

Starting tweet processing...
Processed 10 tweets...
Processed 20 tweets...
...
Processing complete!
Total processed: 50
Records written: 50
Output location: /tmp/twitter_climate
```

#### Option B: With Docker (Full Infrastructure)

```bash
# Start Storm + Redis
docker-compose up -d

# Wait for services to start (30 seconds)
sleep 30

# Run pipeline
bash scripts/run_local.sh
```

---

### Step 6: Verify Output

```bash
# Check Parquet files were created
ls /tmp/twitter_climate/processed_tweets/

# View sample data
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/twitter_climate/processed_tweets/')
print(df.head())
print(f'\nTotal records: {len(df)}')
print(f'Sentiment distribution:\n{df.sentiment_label.value_counts()}')
"
```

---

### Step 7: Create Hive Tables (Optional)

If you have Hive installed:

```bash
# Start Hive
hive --service metastore &
hive --service hiveserver2 &

# Create tables
beeline -u jdbc:hive2://localhost:10000 -f hive/create_tables.sql

# Run queries
beeline -u jdbc:hive2://localhost:10000 -f hive/queries.sql
```

**Skip this if**: You don't have Hive (you can still use pandas to query Parquet files)

---

### Step 8: Generate Visualizations

```bash
# Start Jupyter
jupyter notebook notebooks/visualizations.ipynb

# Or run all cells programmatically
jupyter nbconvert --to notebook --execute notebooks/visualizations.ipynb
```

Visualizations will be saved to `visualizations/` directory.

---

## 🎓 For Assignment Submission

### 1. Prepare Repository

```bash
# Make sure everything is committed
git add .
git commit -m "Complete implementation"
git push

# Make repository public (if private)
# Go to GitHub → Settings → Danger Zone → Change visibility
```

### 2. Create Video Walkthrough

Follow the script in `docs/VIDEO_SCRIPT.md`:

1. Record screen showing:
   - Architecture diagram
   - Pipeline running
   - Parquet files created
   - Visualizations

2. Record yourself explaining:
   - Your contributions
   - Technical challenges
   - Key features

3. Upload to YouTube/Drive and get shareable link

### 3. Complete Report

1. Open `report/REPORT_TEMPLATE.md`
2. Fill in your team details
3. Add your specific results (tweet counts, accuracy, etc.)
4. Add screenshots
5. Export to PDF

### 4. Submit

- GitHub repository URL
- Video link
- Report PDF

---

## 🐛 Troubleshooting

### Issue: `ModuleNotFoundError`

```bash
# Solution: Install missing package
pip install <package-name>

# Or reinstall all
pip install -r requirements.txt
```

### Issue: Models not loading

```bash
# Solution: Download models
python scripts/download_models.py

# Or use VADER fallback (no download needed)
# The pipeline will automatically fall back to VADER
```

### Issue: Permission denied on scripts

```bash
# Solution: Make scripts executable
chmod +x scripts/*.sh
```

### Issue: Redis connection failed

```bash
# Solution: Start Redis with Docker
docker-compose up -d redis

# Or skip Redis (caching will be disabled but pipeline works)
# No action needed - pipeline handles this automatically
```

### Issue: HDFS not available

```bash
# Solution: Use local storage (already default)
export USE_LOCAL_STORAGE=true
export LOCAL_STORAGE_PATH=/tmp/twitter_climate
```

### Issue: Twitter API rate limit

```bash
# Solution: Use sample data
export USE_LOCAL_FILE=true
export LOCAL_SAMPLE_FILE=data/sample_tweets.json
```

---

## 📊 What to Expect

### Processing Sample Data (2 tweets)
- **Time**: ~10 seconds
- **Output**: 2 Parquet files
- **Size**: ~2KB

### Processing 100 Tweets
- **Time**: ~2 minutes
- **Output**: 1 Parquet file
- **Size**: ~50KB

### Processing 1000 Tweets
- **Time**: ~15 minutes
- **Output**: 1-2 Parquet files
- **Size**: ~500KB

---

## 🎯 Minimal Working Example

If you just want to see it work with minimal setup:

```bash
# 1. Install only essential packages
pip install pandas pyarrow

# 2. Create a simple test
cat > test_pipeline.py << 'EOF'
import sys
sys.path.insert(0, 'src')

from spouts.TwitterSpout import TwitterSpout
from bolts.ParseBolt import ParseBolt
from bolts.CleanBolt import CleanBolt

# Initialize
spout = TwitterSpout()
spout.initialize({}, {})

parse_bolt = ParseBolt()
parse_bolt.initialize({}, {})

clean_bolt = CleanBolt()
clean_bolt.initialize({}, {})

# Process one tweet
spout.use_local_file = True
spout.local_file_path = 'data/sample_tweets.json'
spout._load_local_file()

if spout.tweet_buffer:
    tweet = spout.tweet_buffer[0]
    tuple_data = spout._create_tuple(tweet)
    
    parsed = parse_bolt._parse_tweet(tuple_data)
    cleaned = clean_bolt._clean_tweet(parsed)
    
    print("✓ Pipeline working!")
    print(f"Original: {cleaned['text_original']}")
    print(f"Cleaned: {cleaned['text_clean']}")
    print(f"Emoji count: {cleaned['emoji_count']}")
EOF

# 3. Run test
python test_pipeline.py
```

---

## 📚 Next Steps

After getting it running:

1. **Collect More Data**
   ```bash
   python scripts/collect_sample_tweets.py
   ```

2. **Run Full Pipeline**
   ```bash
   python src/topology.py --local
   ```

3. **Analyze Results**
   ```bash
   jupyter notebook notebooks/visualizations.ipynb
   ```

4. **Write Report**
   - Use `report/REPORT_TEMPLATE.md`
   - Add your results and screenshots

5. **Create Video**
   - Follow `docs/VIDEO_SCRIPT.md`
   - Record 5-minute walkthrough

---

## 🆘 Still Having Issues?

### Check System Requirements

```bash
# Python version (need 3.8+)
python --version

# Pip version
pip --version

# Available disk space (need ~5GB for models)
df -h

# Available memory (need ~4GB)
free -h
```

### Run Diagnostics

```bash
# Test imports
python -c "import pandas, pyarrow, emoji; print('✓ Core packages OK')"

# Test file access
ls -la data/sample_tweets.json

# Test write permissions
touch /tmp/test_write && rm /tmp/test_write && echo "✓ Write permissions OK"
```

### Get Help

1. Check `README.md` for detailed documentation
2. Review `QUICK_REFERENCE.md` for common commands
3. See `docs/COMPLETE_PROJECT_SUMMARY.md` for architecture
4. Read code comments in `src/` files

---

## ✅ Success Checklist

- [ ] Repository cloned/pushed to GitHub
- [ ] Python dependencies installed
- [ ] Pipeline runs without errors
- [ ] Parquet files created in output directory
- [ ] Can view processed data with pandas
- [ ] (Optional) Visualizations generated
- [ ] (Optional) Hive tables created
- [ ] Report template filled out
- [ ] Video recorded
- [ ] Ready for submission!

---

## 🎉 You're Done!

Your pipeline is now running. You have:
- ✅ Working Storm topology
- ✅ Processed tweet data
- ✅ Parquet files with sentiment scores
- ✅ Ready for analysis and visualization

**Time to complete**: 10-15 minutes (without models) or 30-45 minutes (with models)

---

**Need help?** Check the troubleshooting section above or review the documentation in `docs/`.

**Ready to submit?** Follow the "For Assignment Submission" section above.

Good luck with your assignment! 🚀
