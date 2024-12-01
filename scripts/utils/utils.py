import logging
import yaml

def read_config(file_path='/opt/airflow/config/config.yml'):
    """
    Reads a YAML configuration file and returns the data as a dictionary.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        dict: Configuration data as a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logging.error(f"Error: Configuration file not found at {file_path}")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        return {}

def read_sql_file(file_path):
    """
    Reads an SQL file and returns its content as a string.

    Args:
        file_path (str): Path to the SQL file to read.

    Returns:
        str: The contents of the SQL file as a string.

    Raises:
        FileNotFoundError: If the file at `file_path` does not exist.
        IOError: If there's an issue reading the file.
    """
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        logging.info(f"Successfully read SQL file from {file_path}.")
        return sql_query

    except FileNotFoundError:
        logging.error(f"Error: The file at {file_path} was not found.")
        return ""
    except IOError as e:
        logging.error(f"Error reading file at {file_path}: {e}")
        return ""

