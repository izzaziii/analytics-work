import pandas as pd
import warnings
from dotenv import load_dotenv
import os
from pathlib import Path

warnings.filterwarnings("ignore", module="openpyxl")

load_dotenv()

BO_REPORT_PATH = os.getenv("BOREPORT_PATH")


class BOReport:
    """
    A class to handle Business Objects report processing.
    """

    def __init__(self, file_path=None):
        """
        Initialize the BOReport object.

        Args:
            file_path (str, optional): Path to the BO report file. Defaults to BO_REPORT_PATH.
        """
        self.file_path = file_path if file_path else BO_REPORT_PATH
        self.data = None
        self.processed_data = None

    def read_data(self):
        """
        Load data from the BO report file.

        Returns:
            pd.DataFrame: The loaded data.
        """
        if not self.file_path:
            print("Error: No file path provided")
            return None

        if not Path(self.file_path).exists():
            print(f"Error: File not found at {self.file_path}")
            return None

        try:
            self.data = pd.read_excel(self.file_path)
            return self.data
        except Exception as e:
            print(f"Error loading data: {e}")
            return None

    def process_data(self):
        """
        Process the loaded data.

        Returns:
            pd.DataFrame: The processed data.
        """
        if self.data is None:
            self.read_data()

        if self.data is not None:
            try:
                self.processed_data = (
                    self.data.loc[
                        (self.data["Funn Status"] != "Lost")
                        & (
                            self.data[" Channel"].isin(
                                ["ONLINE", "INSIDE SALES", "DEALER"]
                            )
                        )
                    ]
                    .astype(
                        {
                            " Channel": "category",
                            "Blk Cluster": "category",
                        }
                    )
                    .assign(
                        date=pd.to_datetime(
                            self.data["Probability 90% Date"],
                            format="%Y-%m-%d",
                            errors="coerce",
                        ),
                        Dob=pd.to_datetime(
                            self.data["Dob"], format="%Y-%m-%d", errors="coerce"
                        ),
                    )
                    .dropna(subset=["date"])
                    .set_index("date")
                )
                return self.processed_data
            except KeyError as e:
                print(f"Error processing data: Column {e} not found in the dataset")
                return None
            except Exception as e:
                print(f"Error processing data: {e}")
                return None
        else:
            print("No data to process")
            return None


if __name__ == "__main__":
    bo_report = BOReport()
    if bo_report.file_path:
        data = bo_report.read_data()
        if data is not None:
            print(f"Data loaded successfully. Shape: {data.shape}")
            print("Sample data:")
            print(data.head())

            processed_data = bo_report.process_data()
            if processed_data is not None:
                print(f"\nProcessed data shape: {processed_data.shape}")
                print("Sample processed data:")
                print(processed_data.head())
            else:
                print("Failed to process data")
    else:
        print(
            "No BO_REPORT_PATH environment variable set. Please set it or provide a file path."
        )
