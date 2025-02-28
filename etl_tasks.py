import logging
import requests
from pyspark.sql import SparkSession

# Global staging area to store API data
staging_data = {}

def fetch_api_data():
    """
    Fetch data from a free API (JSONPlaceholder) and store it in the staging area.
    """
    url = "https://jsonplaceholder.typicode.com/todos"
    logging.info("Fetching data from JSONPlaceholder API...")
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
    data = response.json()
    staging_data['todos'] = data
    logging.info(f"Fetched {len(data)} records from API.")

def pyspark_etl():
    """
    Process the fetched API data using PySpark.
    This example creates a DataFrame from the data, filters tasks that are completed,
    and displays a sample of the results.
    """
    if 'todos' not in staging_data:
        logging.info("No staging data available for transformation. Skipping ETL.")
        return
    logging.info("Starting PySpark ETL process...")
    spark = SparkSession.builder.appName("PySparkETL").getOrCreate()
    # Create DataFrame from the staging data
    df = spark.createDataFrame(staging_data['todos'])
    # Example transformation: Filter tasks that are marked as completed
    df_filtered = df.filter(df.completed == True)
    count = df_filtered.count()
    logging.info(f"Number of completed tasks: {count}")
    df_filtered.show(5)
    spark.stop()
    logging.info("PySpark ETL process completed.")
