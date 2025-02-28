import time
import logging
import threading
from datetime import datetime, timedelta

from scheduler import Scheduler, Task
from etl_tasks import fetch_api_data, pyspark_etl

if __name__ == "__main__":
    # Configure logging format
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    # Create an instance of the scheduler
    scheduler = Scheduler(max_workers=4, check_interval=1)

    # Create a task for fetching API data
    api_task = Task(
        name="FetchAPIData",
        func=fetch_api_data,
        schedule_time=datetime.now(),
        interval=30,  # Run every 30 seconds
        max_retries=3
    )

    # Create a task for PySpark ETL that depends on the API task
    etl_task = Task(
        name="PySparkETLTask",
        func=pyspark_etl,
        schedule_time=datetime.now() + timedelta(seconds=5),
        dependencies=["FetchAPIData"],
        interval=40,  # Run every 40 seconds
        max_retries=1
    )

    # Add tasks to the scheduler
    scheduler.add_task(api_task)
    scheduler.add_task(etl_task)

    # Run the scheduler in a separate thread so the main thread remains responsive
    scheduler_thread = threading.Thread(target=scheduler.start)
    scheduler_thread.start()

    # Let the scheduler run for a certain duration (e.g., 60 seconds) then stop it
    time.sleep(60)
    scheduler.stop()
    scheduler_thread.join()
