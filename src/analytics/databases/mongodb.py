from typing import Dict, List, Any, Optional, Union
from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.collection import Collection
from pymongo.database import Database
import pandas as pd
import json
from datetime import datetime
import logging
from analytics.datasets.boreport import BOReport

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MongoDB")


class MongoDB:
    """
    Class to interact with MongoDB database and collections.
    """

    def __init__(
        self, database: str, collection: str, connection_string: Optional[str] = None
    ) -> None:
        """
        Initialize the MongoDB object.

        Args:
            database (str): Name of the database.
            collection (str): Name of the collection.
            connection_string (Optional[str]): MongoDB connection string. Defaults to None (localhost).
        """
        self.client = (
            MongoClient(connection_string) if connection_string else MongoClient()
        )
        self.db: Database = self.client[database]
        self.collection: Collection = self.db[collection]

    def read_data(self, query: Dict[str, Any] = {}) -> Cursor:
        """
        Read data from the MongoDB collection.

        Args:
            query (Dict[str, Any]): Query filter. Defaults to empty dict (all documents).

        Returns:
            Cursor: A pymongo Cursor object.
        """
        return self.collection.find(query)

    def insert_data(self, data: Union[List[Dict[str, Any]], pd.DataFrame]) -> None:
        """
        Insert data into the MongoDB collection.

        Args:
            data (Union[List[Dict[str, Any]], pd.DataFrame]): The data to insert.
                Can be a list of dictionaries or a pandas DataFrame.

        Raises:
            ValueError: If data is not a list of dictionaries or a pandas DataFrame.
        """
        if isinstance(data, pd.DataFrame):
            # Convert DataFrame to dict and handle NaT values
            records = self._prepare_dataframe_for_mongo(data)
        elif isinstance(data, list) and all(isinstance(item, dict) for item in data):
            records = data
        else:
            raise ValueError(
                "Data must be a pandas DataFrame or a list of dictionaries"
            )

        if records:
            self.collection.insert_many(records)
        else:
            logger.warning("No records to insert")

    def _prepare_dataframe_for_mongo(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to MongoDB compatible format, handling NaT values.

        Args:
            df (pd.DataFrame): DataFrame to convert.

        Returns:
            List[Dict[str, Any]]: List of dictionaries ready for MongoDB insertion.
        """
        # Reset index if it's not default (e.g., if date is the index)
        if not df.index.equals(pd.RangeIndex(len(df))):
            df = df.reset_index()

        # Handle datetime columns to prevent NaT issues
        # First identify datetime columns
        datetime_columns = df.select_dtypes(include=["datetime64"]).columns.tolist()

        # Convert datetime columns to string format before conversion to JSON
        df_copy = df.copy()
        for col in datetime_columns:
            df_copy[col] = df_copy[col].astype(str).replace("NaT", None)

        # Convert to records format
        records = df_copy.to_dict(orient="records")

        return records


if __name__ == "__main__":
    # Example usage
    try:
        # Initialize BOReport object
        logger.info("Initializing BOReport object")
        bo_report = BOReport(
            r"C:\Users\izzaz\Documents\2 Areas\GitHub\analytics-work\data\boreport_test.xlsx"
        )

        # Read and process the data
        logger.info("Reading data from Excel file")
        raw_data = bo_report.read_data()
        if raw_data is None:
            logger.error("Failed to read data from the Excel file")
            exit(1)

        # Initialize MongoDB object
        logger.info("Initializing MongoDB connection")
        mongo = MongoDB(database="deep-diver-v2", collection="boreport")

        # Insert raw data into MongoDB collection
        logger.info(f"Inserting {len(raw_data)} records into MongoDB")
        mongo.insert_data(raw_data)
        logger.info("Data insertion complete")

        # Read data from MongoDB collection
        logger.info("Retrieving sample data from MongoDB")
        cursor = mongo.read_data()
        sample_count = 0
        for i, doc in enumerate(cursor):
            logger.debug(f"Document {i}: {doc}")
            sample_count += 1
            if i >= 2:  # Print only first 3 documents
                break
        logger.info(f"Retrieved {sample_count} sample documents")

    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
