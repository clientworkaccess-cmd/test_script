import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    """Process and clean the downloaded Excel file"""
    
    # Columns to keep
    COLUMNS_TO_KEEP = [
        "CPT* HCPS",
        "Description",
        "Physicians' Fees      North",
        "ASC Fees North"
    ]
    
    # Rename mapping for cleaner column names
    COLUMN_RENAME = {
        "CPT* HCPS": "cpt_hcps",
        "Description": "description",
        "Physicians' Fees      North": "physicians_fees_north",
        "ASC Fees North": "asc_fees_north"
    }
    
    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame"""
        logger.info(f"📖 Reading Excel file: {file_path}")
        try:
            df = pd.read_excel(file_path)
            logger.info(f"✅ Loaded {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"❌ Error reading Excel: {str(e)}")
            raise
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and filter the DataFrame
        - Keep only required columns
        - Rename columns
        - Remove any empty rows
        """
        logger.info("🧹 Cleaning data...")
        
        # Select only required columns
        df_cleaned = df[self.COLUMNS_TO_KEEP].copy()
        
        # Rename columns
        df_cleaned.rename(columns=self.COLUMN_RENAME, inplace=True)
        
        # Remove rows where CPT code is empty
        df_cleaned = df_cleaned.dropna(subset=['cpt_hcps'])
        
        # Remove any completely empty rows
        df_cleaned = df_cleaned.dropna(how='all')
        
        # Reset index
        df_cleaned.reset_index(drop=True, inplace=True)
        
        logger.info(f"✅ Cleaned data: {len(df_cleaned)} rows remaining")
        return df_cleaned
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Main processing pipeline"""
        df = self.read_excel(file_path)
        df_cleaned = self.clean_data(df)
        return df_cleaned