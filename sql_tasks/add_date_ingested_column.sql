-- Add date_ingested column to tides_master if it doesn't exist
ALTER TABLE tides_master 
ADD COLUMN IF NOT EXISTS date_ingested TIMESTAMP WITH TIME ZONE DEFAULT NULL;
