import os
import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    Filter,
    FilterExpression,
    OrderBy,
)
from google.analytics.data_v1beta.types.analytics_data_api import RunReportResponse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("GA4Report")

# Get GA4 property ID from environment variable
GA4_PROPERTY_ID = os.getenv("GOOGLE_ANALYTICS_PROPERTY")


class GA4Report:
    """
    A class to handle Google Analytics 4 data fetching and processing.

    This class provides methods to fetch and process data from Google Analytics 4
    using the Google Analytics Data API v1beta.

    Example usage:

    ```python
    ga4_report = GA4Report()
    data = ga4_report.fetch_data(
        dimensions=["city", "country"],
        metrics=["activeUsers", "sessions"],
        date_range=("7daysAgo", "yesterday")
    )
    ```
    """

    def __init__(self, property_id: Optional[str] = None):
        """
        Initialize the GA4Report object.

        Args:
            property_id (Optional[str]): The GA4 property ID. Defaults to the value from environment variable.

        Raises:
            ValueError: If property_id is not provided and not found in environment variables.
        """
        self.property_id = property_id if property_id else GA4_PROPERTY_ID
        if not self.property_id:
            logger.error(
                "No GA4 property ID provided or found in environment variables"
            )
            raise ValueError(
                "GA4 property ID is required. Set GA4_PROPERTY_ID environment variable or provide it directly."
            )

        self.client = None
        self.data = None
        self.processed_data = None

    def _initialize_client(self) -> None:
        """
        Initialize the GA4 client using application default credentials.

        This method initializes the BetaAnalyticsDataClient which uses the credentials
        specified in the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        """
        try:
            self.client = BetaAnalyticsDataClient()
            logger.info("GA4 client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize GA4 client: {e}")
            raise

    def fetch_data(
        self,
        dimensions: List[str],
        metrics: List[str],
        date_range: tuple = ("7daysAgo", "today"),
        filters: Optional[List[Dict[str, Any]]] = None,
        order_by: Optional[List[Dict[str, Any]]] = None,
        row_limit: int = 10000,
    ) -> pd.DataFrame:
        """
        Fetch data from Google Analytics 4.

        Args:
            dimensions (List[str]): List of dimension names to include in the report.
            metrics (List[str]): List of metric names to include in the report.
            date_range (tuple): Start and end dates for the report. Defaults to ("7daysAgo", "today").
            filters (Optional[List[Dict[str, Any]]]): List of filter dictionaries.
                Each filter should have 'field', 'operator', and 'value' keys.
            order_by (Optional[List[Dict[str, Any]]]): List of order by dictionaries.
                Each should have 'field' and 'desc' (boolean) keys.
            row_limit (int): Maximum number of rows to return. Defaults to 10000.

        Returns:
            pd.DataFrame: DataFrame containing the requested GA4 data.

        Raises:
            ConnectionError: If client initialization fails.
            ValueError: If the request is improperly formed.
            Exception: For any other errors.
        """
        if not self.client:
            try:
                self._initialize_client()
            except Exception as e:
                logger.error(f"Failed to initialize client: {e}")
                raise ConnectionError(f"Failed to initialize GA4 client: {e}")

        try:
            # Prepare dimensions
            dimension_list = [Dimension(name=d) for d in dimensions]

            # Prepare metrics
            metric_list = [Metric(name=m) for m in metrics]

            # Prepare date range
            date_ranges = [DateRange(start_date=date_range[0], end_date=date_range[1])]

            # Prepare filters if provided
            filter_expressions = None
            if filters:
                filter_expressions = self._build_filters(filters)

            # Prepare order by if provided
            order_by_list = None
            if order_by:
                order_by_list = self._build_order_by(order_by)

            # Create the request
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=dimension_list,
                metrics=metric_list,
                date_ranges=date_ranges,
                limit=row_limit,
            )

            # Add filters if available
            if filter_expressions:
                request.dimension_filter = filter_expressions

            # Add order_by if available
            if order_by_list:
                request.order_bys = order_by_list

            # Run the report
            logger.info(
                f"Fetching GA4 data with {len(dimensions)} dimensions and {len(metrics)} metrics"
            )
            response = self.client.run_report(request)

            # Process the response
            self.data = self._process_response(response, dimensions, metrics)
            logger.info(f"Retrieved {len(self.data)} rows from GA4")

            return self.data

        except ValueError as e:
            logger.error(f"Invalid request parameters: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching GA4 data: {e}")
            raise

    def _build_filters(self, filters: List[Dict[str, Any]]) -> FilterExpression:
        """
        Build filter expressions from a list of filter dictionaries.

        Args:
            filters (List[Dict[str, Any]]): List of filter dictionaries.
                Each filter should have 'field', 'operator', and 'value' keys.

        Returns:
            FilterExpression: The constructed filter expression.
        """
        # This is a simplified implementation - a full implementation would handle
        # complex filters with AND/OR logic, but that's beyond scope of this example
        if not filters:
            return None

        # For simplicity, we'll just use the first filter
        filter_dict = filters[0]
        filter_expr = Filter(
            field_name=filter_dict["field"],
            string_filter={
                "match_type": filter_dict["operator"],
                "value": filter_dict["value"],
            },
        )

        return FilterExpression(filter=filter_expr)

    def _build_order_by(self, order_by: List[Dict[str, Any]]) -> List[OrderBy]:
        """
        Build order by list from order by dictionaries.

        Args:
            order_by (List[Dict[str, Any]]): List of order by dictionaries.
                Each should have 'field' and 'desc' (boolean) keys.

        Returns:
            List[OrderBy]: List of OrderBy objects.
        """
        order_by_list = []
        for order in order_by:
            if order.get("field"):
                order_by_obj = OrderBy(
                    dimension={"dimension_name": order["field"]}
                    if "dimension:" in order["field"]
                    else None,
                    metric={"metric_name": order["field"]}
                    if "metric:" in order["field"]
                    else None,
                    desc=order.get("desc", False),
                )
                order_by_list.append(order_by_obj)

        return order_by_list

    def _process_response(
        self, response: RunReportResponse, dimensions: List[str], metrics: List[str]
    ) -> pd.DataFrame:
        """
        Process the GA4 response into a pandas DataFrame.

        Args:
            response (RunReportResponse): The response from the GA4 API.
            dimensions (List[str]): List of dimension names.
            metrics (List[str]): List of metric names.

        Returns:
            pd.DataFrame: DataFrame containing the processed data.
        """
        rows = []

        for row in response.rows:
            row_dict = {}

            # Add dimensions
            for i, dimension in enumerate(row.dimension_values):
                row_dict[dimensions[i]] = dimension.value

            # Add metrics
            for i, metric in enumerate(row.metric_values):
                row_dict[metrics[i]] = (
                    float(metric.value)
                    if metric.value.replace(".", "", 1).isdigit()
                    else metric.value
                )

            rows.append(row_dict)

        return pd.DataFrame(rows)

    def process_data(
        self, custom_processing: Optional[callable] = None
    ) -> pd.DataFrame:
        """
        Process the fetched data with optional custom processing function.

        Args:
            custom_processing (Optional[callable]): A function that takes a DataFrame and returns a processed DataFrame.

        Returns:
            pd.DataFrame: The processed DataFrame.

        Raises:
            ValueError: If no data has been fetched yet.
        """
        if self.data is None:
            logger.error("No data to process. Call fetch_data() first")
            raise ValueError("No data to process. Call fetch_data() first")

        try:
            if custom_processing:
                self.processed_data = custom_processing(self.data)
            else:
                # Default processing - just return the data as is
                self.processed_data = self.data

            logger.info(
                f"Data processed successfully. Shape: {self.processed_data.shape}"
            )
            return self.processed_data

        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise


def _get_cli_input(
    prompt: str, options: Optional[List[str]] = None, allow_multiple: bool = False
) -> Union[str, List[str]]:
    """
    Get validated input from the command line.

    Args:
        prompt (str): The prompt to show the user
        options (Optional[List[str]]): List of valid options, if any
        allow_multiple (bool): Whether to allow multiple comma-separated values

    Returns:
        Union[str, List[str]]: The validated input (string or list of strings)
    """
    while True:
        if options:
            option_str = ", ".join(options)
            input_str = input(f"{prompt} [{option_str}]: ")
        else:
            input_str = input(f"{prompt}: ")

        if not input_str.strip():
            print("Input cannot be empty. Please try again.")
            continue

        if allow_multiple:
            values = [val.strip() for val in input_str.split(",")]

            if options:
                invalid_values = [val for val in values if val not in options]
                if invalid_values:
                    print(
                        f"Invalid values: {', '.join(invalid_values)}. Please try again."
                    )
                    continue

            return values
        else:
            if options and input_str not in options:
                print(f"Invalid input. Please choose from: {', '.join(options)}")
                continue

            return input_str


def run_interactive_cli() -> None:
    """
    Run an interactive CLI for fetching GA4 data.
    """
    print("\n===== Google Analytics 4 Data Fetcher =====\n")

    # Common GA4 dimensions and metrics to suggest
    common_dimensions = [
        "date",
        "deviceCategory",
        "country",
        "city",
        "browser",
        "operatingSystem",
        "pagePath",
        "pageTitle",
        "sessionSource",
        "sessionMedium",
        "sessionCampaignName",
    ]

    common_metrics = [
        "screenPageViews",
        "totalUsers",
        "newUsers",
        "activeUsers",
        "sessions",
        "engagementRate",
        "averageSessionDuration",
        "conversions",
        "eventCount",
    ]

    common_date_ranges = [
        "7daysAgo",
        "14daysAgo",
        "30daysAgo",
        "90daysAgo",
        "yesterday",
        "today",
    ]

    try:
        # Let user choose dimensions
        print("\nChoose dimensions (comma-separated):")
        print("Common options: " + ", ".join(common_dimensions))
        dimensions = _get_cli_input("Dimensions", allow_multiple=True)

        # Let user choose metrics
        print("\nChoose metrics (comma-separated):")
        print("Common options: " + ", ".join(common_metrics))
        metrics = _get_cli_input("Metrics", allow_multiple=True)

        # Let user choose date range
        print("\nChoose date range:")
        print("Common options: " + ", ".join(common_date_ranges))
        print("You can also use YYYY-MM-DD format")
        start_date = _get_cli_input("Start date")
        end_date = _get_cli_input("End date")

        # Let user choose row limit
        row_limit = int(_get_cli_input("Maximum rows to return (default 100)") or "100")

        print("\nFetching data from Google Analytics 4...")

        # Initialize GA4 client
        ga4_report = GA4Report()

        # Fetch data
        data = ga4_report.fetch_data(
            dimensions=dimensions,
            metrics=metrics,
            date_range=(start_date, end_date),
            row_limit=row_limit,
        )

        # Display results
        print("\n===== Results =====\n")
        print(f"Retrieved {len(data)} rows of GA4 data")

        # If too many columns for display, ask which ones to show
        if len(data.columns) > 10:
            print(f"\nThere are {len(data.columns)} columns in the result.")
            print("Available columns: " + ", ".join(data.columns))
            display_cols = _get_cli_input(
                "Which columns to display (comma-separated, or 'all')",
                allow_multiple=True,
            )

            if display_cols == ["all"]:
                print("\nFull data:")
                print(data)
            else:
                valid_cols = [col for col in display_cols if col in data.columns]
                print("\nData for selected columns:")
                print(data[valid_cols])
        else:
            print("\nFull data:")
            print(data)

        # Ask if user wants to save the data to a CSV
        save_option = _get_cli_input("Save data to CSV? (yes/no)", ["yes", "no"])

        if save_option.lower() == "yes":
            filename = _get_cli_input("Enter filename (without .csv extension)")
            filepath = f"{filename}.csv"
            data.to_csv(filepath, index=False)
            print(f"Data saved to {filepath}")

        print("\nThank you for using Deep Diver Data!")

    except ValueError as e:
        logger.error(f"Input error: {e}")
        print(f"Error: {e}")
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.error(f"Error in GA4Report CLI: {e}", exc_info=True)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    run_interactive_cli()
