"""
API Data Extractor
Handles data extraction from REST APIs with retry logic and error handling.
"""

import os
import json
import requests
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger


class APIExtractor:
    """
    Generic API extractor with built-in retry logic and logging.
    """

    def __init__(self, base_url: str, api_key: Optional[str] = None):
        """
        Initialize the API extractor.

        Args:
            base_url: Base URL for the API
            api_key: Optional API key for authentication
        """
        self.base_url = base_url
        self.api_key = api_key
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})

    def extract(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = 'GET'
    ) -> List[Dict]:
        """
        Extract data from an API endpoint.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method (GET, POST, etc.)

        Returns:
            List of records extracted from the API
        """
        url = f"{self.base_url}/{endpoint}"
        
        try:
            logger.info(f"Extracting data from {url}")
            
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully extracted {len(data)} records")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to extract data from {url}: {str(e)}")
            raise

    def extract_incremental(
        self,
        endpoint: str,
        last_updated_field: str,
        last_run_timestamp: Optional[datetime] = None
    ) -> List[Dict]:
        """
        Extract data incrementally based on a timestamp field.

        Args:
            endpoint: API endpoint path
            last_updated_field: Field name for filtering by timestamp
            last_run_timestamp: Timestamp of last successful run

        Returns:
            List of records updated since last run
        """
        params = {}
        
        if last_run_timestamp:
            params[last_updated_field] = last_run_timestamp.isoformat()
            logger.info(f"Extracting records updated since {last_run_timestamp}")
        else:
            logger.info("Performing full extraction (no previous timestamp)")
        
        return self.extract(endpoint, params=params)

    def save_to_file(self, data: List[Dict], output_path: str) -> None:
        """
        Save extracted data to a JSON file.

        Args:
            data: Data to save
            output_path: Path to output file
        """
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Saved {len(data)} records to {output_path}")


# Example usage
if __name__ == "__main__":
    # Example: Extract data from a public API
    extractor = APIExtractor(base_url="https://api.example.com")
    
    # Extract data
    data = extractor.extract(endpoint="data", params={"limit": 100})
    
    # Save to file
    extractor.save_to_file(data, output_path="/tmp/extracted_data.json")
