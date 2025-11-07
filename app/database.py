from supabase import create_client, Client
from typing import List, Dict
import logging
import os

logger = logging.getLogger(__name__)

class SupabaseHandler:
    """Handle Supabase database operations"""
    
    def __init__(self):
        # Load from environment variables
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        self.table_name = "test_cleaning"
        
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("Missing Supabase credentials in environment variables")
        
        self.client: Client = create_client(self.supabase_url, self.supabase_key)
        logger.info("✅ Supabase client initialized")
    
    def insert_records(self, records: List[Dict]) -> dict:
        """
        Insert multiple records into Supabase
        Returns: Summary of insertion results
        """
        logger.info(f"📤 Inserting {len(records)} records into '{self.table_name}'...")
        
        try:
            # Clear existing data (optional - remove if you want to append)
            # self.client.table(self.table_name).delete().neq('id', 0).execute()
            
            # Insert in batches (Supabase handles bulk inserts well)
            response = self.client.table(self.table_name).insert(records).execute()
            
            logger.info(f"✅ Successfully inserted {len(records)} records")
            
            return {
                "status": "success",
                "records_inserted": len(records),
                "table": self.table_name
            }
            
        except Exception as e:
            logger.error(f"❌ Error inserting records: {str(e)}")
            raise
    
    def insert_records_one_by_one(self, records: List[Dict]) -> dict:
        """
        Insert records one by one (slower but more fault-tolerant)
        """
        logger.info(f"📤 Inserting {len(records)} records one by one...")
        
        success_count = 0
        failed_count = 0
        errors = []
        
        for idx, record in enumerate(records, 1):
            try:
                self.client.table(self.table_name).insert(record).execute()
                success_count += 1
                
                if idx % 100 == 0:
                    logger.info(f"Progress: {idx}/{len(records)} records processed")
                    
            except Exception as e:
                failed_count += 1
                errors.append({"record_index": idx, "error": str(e)})
                logger.warning(f"Failed to insert record {idx}: {str(e)}")
        
        logger.info(f"✅ Insertion complete: {success_count} success, {failed_count} failed")
        
        return {
            "status": "completed",
            "records_inserted": success_count,
            "records_failed": failed_count,
            "table": self.table_name,
            "errors": errors[:10]  # Return first 10 errors
        }