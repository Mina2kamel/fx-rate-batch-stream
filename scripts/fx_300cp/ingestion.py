import logging
from io import BytesIO
import numpy as np
import pandas as pd
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from scripts.utils.utils import read_config

CONFIG = read_config()

# Constants
NUM_ROWS = CONFIG['fx_rates']['fx_300cp_num_rows']
MILLISECONDS_IN_A_DAY = 86_400_000

# Define 300 currency pairs
CURRENCY_PAIRS = ["EURUSD", "NZDUSD", "GBPUSD", "USDJPY", "AUDUSD"] + [
    f"CUR{i}CUR{j}" for i in range(1, 16) for j in range(16, 36)
][:295]  # Randomly generated 295 more pairs


def generate_fx_sample_data(num_rows: int) -> pd.DataFrame:
    """
    Generates a large dataset of foreign exchange rates.

    Args:
        num_rows (int): Number of rows to generate.

    Returns:
        pd.DataFrame: A DataFrame containing random FX rate data.
    """
    try:
        # Current time in milliseconds
        current_time = int(datetime.now().timestamp() * 1000)

        # Generate random event times
        event_times_last_day = np.random.randint(
            current_time - MILLISECONDS_IN_A_DAY,
            current_time,
            size=num_rows // 2)

        event_times_previous_day = np.random.randint(
            current_time - 2 * MILLISECONDS_IN_A_DAY,
            current_time - MILLISECONDS_IN_A_DAY,
            size=num_rows // 2)

        # Combine and shuffle event times
        event_times = np.concatenate([event_times_last_day, event_times_previous_day])
        np.random.shuffle(event_times)

        # Generate other random data
        event_ids = np.arange(1, num_rows + 1)
        ccy_couples = np.random.choice(CURRENCY_PAIRS, size=num_rows)
        rates = np.round(np.random.uniform(0.5, 2.0, size=num_rows), 5)

        # Create and return a DataFrame
        data = pd.DataFrame({
            "event_id": event_ids,
            "event_time": event_times,
            "ccy_couple": ccy_couples,
            "rate": rates
        })
        logging.info(f"Generated {num_rows} rows of FX sample data.")
        return data
    except Exception as e:
        logging.error(f"Error generating FX sample data: {e}")
        raise ValueError("Failed to generate FX sample data.") from e


def upload_to_minio_parquet(bucket_name: str) -> str:
    """
    Uploads a DataFrame to a MinIO S3 bucket as a Parquet file.

    Args:
        bucket_name (str): Name of the MinIO bucket.

    Returns:
        str: The name of the uploaded file.
    """
    if not bucket_name:
        raise ValueError("Bucket name cannot be None or empty.")

    try:
        # MinIO client configuration
        minio_client = Minio(
            endpoint=CONFIG['minio']['endpoint'],
            access_key=CONFIG['minio']['access_key'],
            secret_key=CONFIG['minio']['secret_key'],
            secure=False
        )

        # Ensure the bucket exists
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logging.info(f"Created bucket: {bucket_name}")

        # Generate a unique filename
        current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"fx_data_{current_date}.parquet"

        # Generate and save data
        data = generate_fx_sample_data(NUM_ROWS)
        parquet_buffer = BytesIO()
        data.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        # Upload to MinIO
        minio_client.put_object(
            bucket_name,
            file_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        logging.info(f"File uploaded to MinIO bucket '{bucket_name}' as '{file_name}'")
        return file_name
    except S3Error as e:
        logging.error(f"MinIO S3 error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error uploading Parquet file to MinIO: {e}")
        raise RuntimeError("Failed to upload data to MinIO.") from e
