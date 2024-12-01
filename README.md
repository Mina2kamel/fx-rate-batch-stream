# FX Exchange Rates Batch & Streaming Processing Pipeline

## Task 1: 5 Currency Pairs
An Airflow DAG is designed to schedule the batch processing job every hour. The workflow consists of three main tasks: ingestion, query, and monitoring.

### Ingestion
* The ingestion process involves generating sample data with random values to simulate FX exchange rates for five predefined currency pairs.
* To simplify the solution, the data includes records from the last 24 hours, shuffled to create variety, and totals 500 entries.
* This raw data is saved into a PostgreSQL raw table to facilitate querying during the processing phase.

### Query
* The query phase utilizes subqueries to process the data effectively.
* One subquery selects the required fields and filters for events that occurred within the last 30 seconds, while another filters for events from yesterday at 5 PM.
* Using this data, the rate change percentages are calculated with the formula `(current_rate - yesterday_rate) / yesterday_rate`.
* The processed data, including these calculated percentages, is then saved into a PostgreSQL processed table.

### Monitoring
* The processed data is visualized using Streamlit, which displays the results in a table format.
* This interface serves as a simulation of screen-led monitoring for easy review of the exchange rate changes.

## Task 2: 300 Currency Pairs
An Airflow DAG is designed to schedule the batch processing job every minute. The workflow consists of three main tasks: ingestion, query, and monitoring.

### Ingestion
* The ingestion process involves generating sample data to simulate FX exchange rates for 300 currency pairs, creating a dataset of 5 million records to emulate big data scenarios.
* This data is stored in an S3-compatible MinIO bucket named `raw-fx-rates` in Parquet format, providing a scalable and efficient storage solution.

### Processing
* The processing stage uses PySpark to handle and analyze the raw FX rates data.
* It begins by retrieving the file name stored in the MinIO bucket from the ingestion task and initializing a Spark session with the necessary configurations.
* The raw Parquet file is loaded into a Spark DataFrame, where the data is filtered to include only records from the last 30 seconds.
* The filtered data is then aggregated to extract the latest event time and corresponding rate for each currency pair.
* If the current time is 5 PM (17:00), the aggregated data is saved into a PostgreSQL database reference table as a reference for calculating percentage changes.
* The percentage change is computed using the formula `(current_rate - yesterday_rate) / yesterday_rate`, and the processed data, including these calculations, is stored in a PostgreSQL processed table.

### Monitoring
* For monitoring, a Streamlit dashboard is used to visualize the processed FX exchange rates stored in the PostgreSQL database.
* This dashboard provides a user-friendly interface to display the results, making it easier to track and analyze the processed data in real-time.

## Task 3: Streaming Solution

### Kafka:
* Sample data is generated for 5 currency pairs and published by a producer to a Kafka topic.
* The data is serialized every 5 minutes to simulate real-time streaming.

### Spark Streaming:
* Spark Streaming is used to consume events from the Kafka topic, starting with the earliest offset to ensure only new events are processed.
* These events are then parsed into a DataFrame.
* A watermark of 1 minute is applied to handle late-arriving data, enabling the extraction of the latest event.
* Events from the last 30 minutes are filtered, and the percentage change is calculated by simulating reference data.
* The reference data is broadcast-joined with the processed rates to compute the percentage change.
* Finally, the results are written to a PostgreSQL stream table.

### Grafana:
* Grafana can be used to monitor the processed data in real time, providing a visual representation of the streaming data as it is processed.
