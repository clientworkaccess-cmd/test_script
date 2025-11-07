from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pathlib import Path
import logging
from datetime import datetime
import pandas as pd

from app.scraper import NJMedicalScraper
from app.data_processor import DataProcessor
from app.database import SupabaseHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(
    title="NJ Medical Fee Schedule Scraper API",
    description="API to scrape, process, and store NJ medical fee schedules",
    version="1.0.0"
)

# Health check endpoint
@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "NJ Medical Fee Schedule Scraper API",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/scrape-and-store")
async def scrape_and_store(
    background_tasks: BackgroundTasks = None,
    insert_method: str = "bulk"  # "bulk" or "one_by_one"
):
    """
    Main endpoint to:
    1. Download Excel file from NJ website
    2. Process and clean the data
    3. Insert into Supabase
    
    Query params:
    - insert_method: "bulk" (faster) or "one_by_one" (more fault-tolerant)
    """
    try:
        logger.info("🚀 Starting scrape and store process...")
        
        # Step 1: Download Excel file
        scraper = NJMedicalScraper()
        file_path = scraper.download_excel_file(headless=True)
        logger.info(f"✅ Downloaded file: {file_path}")
        
        # Step 2: Process data
        processor = DataProcessor()
        df_cleaned = processor.process_file(file_path)
        logger.info(f"✅ Processed {len(df_cleaned)} records")
        
        # Convert DataFrame to list of dictionaries
        records = df_cleaned.to_dict('records')
        
        # Step 3: Insert into Supabase
        db_handler = SupabaseHandler()
        
        if insert_method == "one_by_one":
            result = db_handler.insert_records_one_by_one(records)
        else:
            result = db_handler.insert_records(records)
        
        # Clean up downloaded file
        file_path.unlink(missing_ok=True)
        logger.info("🧹 Cleaned up temporary file")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Data scraped and stored successfully",
                "details": result,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    except Exception as e:
        logger.error(f"❌ Error in scrape_and_store: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "message": "Failed to scrape and store data"
            }
        )

@app.get("/test-download")
async def test_download():
    """Test endpoint to just download and return file info"""
    try:
        scraper = NJMedicalScraper()
        file_path = scraper.download_excel_file(headless=True)
        
        # Read basic info
        processor = DataProcessor()
        df = processor.read_excel(file_path)
        
        # Clean up
        file_path.unlink(missing_ok=True)
        
        return {
            "status": "success",
            "rows": len(df),
            "columns": list(df.columns),
            "sample_data": df.head(3).to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test-supabase")
async def test_supabase():
    """Test Supabase connection"""
    try:
        db_handler = SupabaseHandler()
        return {
            "status": "success",
            "message": "Supabase connection successful",
            "table": db_handler.table_name
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))