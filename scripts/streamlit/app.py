import streamlit as st
from scripts.fx_300cp.spark_processing import load_reference_rates
from scripts.utils.spark_manager import create_spark_connection
from scripts.utils.utils import read_config

# Load configuration
CONFIG = read_config()

# Constants
PROCESSED_RATES_TABLE = CONFIG['postgres']['processed_table']

def run_app(df):
    """
    Streamlit app to display currency pair rates in an LED-style table.

    Args:
        df (pd.DataFrame): A Pandas DataFrame containing currency rates.
    """
    # Streamlit app
    st.title("Currency Pair Rates Simulation")
    st.write("Live currency rates displayed in an LED-style format.")

    # Style for LED-like display
    st.markdown(
        """
        <style>
        .led-table {
            font-family: monospace;
            color: lime;
            background-color: black;
            text-align: center;
            font-size: 20px;
            padding: 10px;
            border-collapse: collapse;
            width: 100%;
        }
        .led-table th, .led-table td {
            border: 1px solid lime;
            padding: 10px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Display the data in an LED-style table
    st.markdown(
        f"""
        <table class="led-table">
            <tr>
                <th>Currency Pair</th>
                <th>Rate</th>
                <th>Change</th>
            </tr>
            {''.join(
                f"<tr><td>{row['ccy_couple']}</td><td>{row['current_rate']}</td><td>{row['change']}</td></tr>"
                for row in df.to_dict('records')
            )}
        </table>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    # Establish a Spark connection
    spark = create_spark_connection()

    # Load data from Spark and convert it to Pandas DataFrame
    spark_df = load_reference_rates(spark, PROCESSED_RATES_TABLE)

    # Ensure conversion to Pandas DataFrame
    df = spark_df.toPandas()

    # Run the Streamlit app
    run_app(df)
