import logging
import json
import threading
import requests

from datetime import datetime, timedelta
from functools import partial
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from scheduler import Scheduler, Task

# We'll store user-created pipeline configs in memory for this demo
PIPELINE_CONFIGS = {}
CONFIG_ID_COUNTER = 1

app = FastAPI(title="DataPlatform UI")
templates = Jinja2Templates(directory="templates")

# Create a global scheduler instance
scheduler = Scheduler(max_workers=4, check_interval=1)
scheduler_thread = None

@app.on_event("startup")
def startup_event():
    """
    Start the scheduler in a background thread when the FastAPI app starts.
    """
    global scheduler_thread
    if not scheduler_thread:
        scheduler_thread = threading.Thread(target=scheduler.start, daemon=True)
        scheduler_thread.start()
        logging.info("Scheduler started on FastAPI startup.")

@app.on_event("shutdown")
def shutdown_event():
    """
    Stop the scheduler when the FastAPI app shuts down.
    """
    scheduler.stop()
    logging.info("Scheduler stopped on FastAPI shutdown.")

# -------------------------------------------------------
# A function that can handle dynamic ETL logic
# -------------------------------------------------------
def dynamic_etl(config_id: int):
    """
    Fetch data from a user-defined API, then store it based on the config.
    """
    config = PIPELINE_CONFIGS.get(config_id)
    if not config:
        logging.error(f"No pipeline config found for ID: {config_id}")
        return

    api_url = config["api_url"]
    destination = config["destination"]
    file_name = config.get("file_name", "output.json")
    db_name = config.get("db_name", "MyDB")

    # 1. Fetch data
    logging.info(f"[dynamic_etl] Fetching data from {api_url}")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logging.error(f"[dynamic_etl] Error fetching data from {api_url}: {e}")
        return

    # 2. Store data
    if destination == "file":
        try:
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logging.info(f"[dynamic_etl] Data saved to file: {file_name}")
        except Exception as e:
            logging.error(f"[dynamic_etl] Error saving data to file: {e}")
    elif destination == "db":
        # Here, you’d normally connect to a real DB and insert data
        # For this demo, we just log it
        logging.info(f"[dynamic_etl] Simulating DB insert into '{db_name}' with {len(data)} records.")
    else:
        logging.warning("[dynamic_etl] Unknown destination. Doing nothing.")

# -------------------------------------------------------
# Main Page: just a link to create tasks
# -------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index_page(request: Request):
    """
    Simple landing page with a link to create a custom pipeline or see results.
    """
    return templates.TemplateResponse("index.html", {"request": request})

# -------------------------------------------------------
# Form to Create a Custom ETL Task
# -------------------------------------------------------
@app.get("/create_custom_task", response_class=HTMLResponse)
def create_custom_task_form(request: Request):
    """
    Show a form where the user can input:
      - Task Name
      - API URL
      - Interval
      - Dependencies
      - Destination (File or DB)
      - File Name or DB Name
    """
    return templates.TemplateResponse("create_custom_task.html", {"request": request})

@app.post("/create_custom_task", response_class=HTMLResponse)
def create_custom_task_submit(
    request: Request,
    task_name: str = Form(...),
    api_url: str = Form(...),
    interval: int = Form(...),
    dependencies: str = Form(""),
    destination: str = Form(...),
    file_name: str = Form(""),
    db_name: str = Form("")
):
    """
    Handle the form submission to create a new dynamic ETL task in the scheduler.
    """
    global CONFIG_ID_COUNTER

    # Convert dependencies string (comma-separated) into a list
    dep_list = [d.strip() for d in dependencies.split(",") if d.strip()]

    # 1. Store pipeline config in memory
    config_id = CONFIG_ID_COUNTER
    CONFIG_ID_COUNTER += 1

    PIPELINE_CONFIGS[config_id] = {
        "api_url": api_url,
        "destination": destination,
        "file_name": file_name,
        "db_name": db_name
    }

    # 2. Create the actual Task object
    # We'll use partial() so the dynamic_etl function knows which config_id to load
    func_with_config = partial(dynamic_etl, config_id=config_id)
    new_task = Task(
        name=task_name,
        func=func_with_config,
        schedule_time=datetime.now() + timedelta(seconds=5),
        interval=interval,
        dependencies=dep_list,
        max_retries=2
    )

    # 3. Add the task to the scheduler
    scheduler.add_task(new_task)

    msg = (
        f"Created task '{task_name}' to fetch from {api_url} every {interval} seconds. "
        f"Destination='{destination}'. Dependencies={dep_list}"
    )
    return templates.TemplateResponse("result.html", {"request": request, "result": msg})
