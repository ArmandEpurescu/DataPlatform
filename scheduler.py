import time
import logging
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# Task Class Definition
# -----------------------------
class Task:
    def __init__(self, name, func, schedule_time=None, interval=None, dependencies=None, max_retries=0):
        """
        :param name: Unique name of the task.
        :param func: Callable function representing the task's action.
        :param schedule_time: Datetime when the task is scheduled to run.
        :param interval: If provided, the task is periodic with this interval (in seconds).
        :param dependencies: List of task names that must be completed before this task runs.
        :param max_retries: Maximum number of retries if the task fails.
        """
        self.name = name
        self.func = func
        self.schedule_time = schedule_time or datetime.now()
        self.interval = interval
        self.dependencies = dependencies or []
        self.max_retries = max_retries
        self.retry_count = 0
        self.last_run = None
        self.state = 'PENDING'  # Possible states: PENDING, RUNNING, SUCCESS, FAILED
        self.lock = threading.Lock()

    def run(self):
        """
        Executes the task's function and handles state transitions.
        """
        with self.lock:
            self.state = 'RUNNING'
        logging.info(f"[{datetime.now()}] Task '{self.name}' started execution.")
        try:
            self.func()
            with self.lock:
                self.state = 'SUCCESS'
                self.last_run = datetime.now()
            logging.info(f"[{self.last_run}] Task '{self.name}' completed successfully.")
            return True
        except Exception as e:
            with self.lock:
                self.retry_count += 1
                self.state = 'FAILED'
            logging.error(f"[{datetime.now()}] Task '{self.name}' failed on attempt {self.retry_count}: {e}")
            return False

    def reset(self):
        """
        Resets the task state for periodic tasks or retry attempts.
        """
        with self.lock:
            self.state = 'PENDING'

# -----------------------------
# Scheduler Class Definition
# -----------------------------
class Scheduler:
    def __init__(self, max_workers=4, check_interval=1):
        self.tasks = {}  # Dictionary mapping task names to Task instances
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.check_interval = check_interval  # Interval (in seconds) to check task readiness
        self.shutdown_flag = threading.Event()

    def add_task(self, task: Task):
        """
        Add a new task to the scheduler.
        """
        with self.lock:
            self.tasks[task.name] = task
        logging.info(f"Task '{task.name}' added, scheduled for {task.schedule_time}.")

    def can_run(self, task: Task):
        """
        Determine if the task can run based on its schedule and dependencies.
        """
        if datetime.now() < task.schedule_time:
            return False
        with self.lock:
            for dep_name in task.dependencies:
                dep_task = self.tasks.get(dep_name)
                if not dep_task or dep_task.state != 'SUCCESS':
                    return False
        return True

    def schedule_task(self, task: Task):
        """
        Submit a task to the thread pool executor for execution.
        """
        future = self.executor.submit(task.run)
        return future

    def start(self):
        """
        Main scheduler loop that periodically checks and submits tasks.
        """
        logging.info("Scheduler started.")
        futures = {}  # Map task names to their Future objects
        while not self.shutdown_flag.is_set():
            with self.lock:
                for task in list(self.tasks.values()):
                    # Check tasks in PENDING or FAILED state (for retry)
                    if task.state in ['PENDING', 'FAILED']:
                        if self.can_run(task):
                            if task.state == 'FAILED' and task.retry_count >= task.max_retries:
                                logging.warning(f"Task '{task.name}' reached maximum retries. Skipping further attempts.")
                                continue
                            if task.name not in futures or futures[task.name].done():
                                logging.info(f"Submitting task '{task.name}' for execution.")
                                futures[task.name] = self.schedule_task(task)
            # Check for periodic tasks to reschedule
            for name, future in list(futures.items()):
                if future.done():
                    with self.lock:
                        task = self.tasks.get(name)
                        if task and task.interval and task.state == 'SUCCESS':
                            task.schedule_time = datetime.now() + timedelta(seconds=task.interval)
                            task.reset()
                            logging.info(f"Task '{name}' rescheduled to run at {task.schedule_time}.")
            time.sleep(self.check_interval)
        self.executor.shutdown(wait=True)
        logging.info("Scheduler stopped.")

    def stop(self):
        """
        Signal the scheduler to shut down.
        """
        self.shutdown_flag.set()
