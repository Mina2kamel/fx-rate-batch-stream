import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from scripts.utils.utils import read_config

CONFIG = read_config()
db_config = CONFIG['db']
NUM_ROWS = CONFIG['fx_rates']['fx_5cp_num_rows']
RAW_RATES_TABLE = CONFIG['postgres']['raw_table']

CURRENCY_PAIRS = ["EURUSD", "NZDUSD", "GBPUSD", "USDJPY", "AUDUSD"]
MILLISECONDS_IN_A_DAY = 86_400_000


def generate_fx_sample_data(num_rows):
    """
    Generate sample FX rate data for the past two days.

    Args:
        num_rows (int): Number of rows to generate.

    Returns:
        pd.DataFrame: A DataFrame containing the generated FX data.
    """
    try:
        current_time = int(datetime.now().timestamp() * 1000)
        event_times = np.random.randint(
            current_time - 2 * MILLISECONDS_IN_A_DAY,
            current_time,
            size=num_rows
        )
        np.random.shuffle(event_times)

        data = pd.DataFrame({
            "event_id": np.arange(1, num_rows + 1),
            "event_time": event_times,
            "ccy_couple": np.random.choice(CURRENCY_PAIRS, size=num_rows),
            "rate": np.round(np.random.uniform(0.5, 2.0, size=num_rows), 5)
        })
        logging.info(f"Generated {num_rows} rows of FX sample data.")
        return data

    except Exception as e:
        logging.error(f"Error generating FX sample data: {e}")
        return pd.DataFrame()


def load_data_to_postgres():
    """
    Load generated FX data into the PostgreSQL database.
    """
    try:
        data = generate_fx_sample_data(NUM_ROWS)
        if data.empty:
            logging.warning("No data to load into PostgreSQL.")
            return

        connection_string = (
            f"{db_config['dialect']}+{db_config['driver']}://{db_config['username']}:"
            f"{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        engine = create_engine(connection_string)

        data.to_sql(RAW_RATES_TABLE, engine, if_exists="replace", index=False)
        logging.info("Data successfully loaded into PostgreSQL.")

    except SQLAlchemyError as db_error:
        logging.error(f"Database error: {db_error}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
