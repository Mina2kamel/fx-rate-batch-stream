import time
import random
import json
import logging
from kafka import KafkaProducer
from scripts.utils.utils import read_config

# Load configuration
CONFIG = read_config()
KAFKA_TOPIC = CONFIG['kafka']['KAFKA_TOPIC']
BOOTSTRAP_SERVER = CONFIG['kafka']['KAFKA_SERVER']

# Currency pairs to simulate
CURRENCY_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "NZDUSD"]

def generate_fx_rate(ccy_couple):
    """
    Generate synthetic FX rate data for a given currency pair.

    Args:
        ccy_couple (str): Currency pair (e.g., 'EURUSD').

    Returns:
        dict: Simulated FX rate event.
    """
    return {
        "event_id": random.randint(1e9, 1e10),  # Random unique ID
        "event_time": int(time.time() * 1000),  # Current epoch time in milliseconds
        "ccy_couple": ccy_couple,
        "rate": round(random.uniform(0.5, 1.5), 6)  # Random FX rate
    }

def produce_fx_rate(producer, ccy_couple):
    """
    Produce a single FX rate message to the Kafka topic.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        ccy_couple (str): Currency pair (e.g., 'EURUSD').
    """
    try:
        fx_rate = generate_fx_rate(ccy_couple)
        producer.send(
            topic=KAFKA_TOPIC,
            value=json.dumps(fx_rate).encode('utf-8')
        )
    except Exception as e:
        logging.error(f"Error producing message for {ccy_couple}: {e}")

def stream_fx_rates():
    """
    Main streaming function to produce FX rate data.
    """
    producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER], max_block_ms=5000)
    start_time = time.time()

    while time.time() <= start_time + 5 * 60:  # Run for 5 minutes
        for ccy in CURRENCY_PAIRS:
            produce_fx_rate(producer, ccy)

    producer.close()
