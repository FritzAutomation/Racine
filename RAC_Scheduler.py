"""
Combined Scheduler Module

This module creates the schedules for the following functions below.

Author: [Joshua Fritzjunker]
Email: [Joshua.Fritzjunker@Cnhind.com]
Date: [11/13/24]
"""

import schedule
import time as time_module
import datetime
import logging
import subprocess
from retrying import retry

logging.basicConfig(
    filename=r"logs\rac_scheduler.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define the path of the file where you want to store the timestamp
timestamp_file = r"temp\rac_scheduler_timestamp.txt"


@retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
)
def execute_script(script_name):
    start_time = datetime.datetime.now()
    logging.info(f"{script_name} started at {start_time}")
    try:
        subprocess.run(["python", script_name], check=True)
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logging.info(f"{script_name} ended at {end_time} after running for {duration}")
    except subprocess.CalledProcessError as e:
        logging.warning(f"Error executing {script_name}: {e}")
        raise  # Important: you need to re-raise the exception to trigger the retry


# Define all your functions here
def test_time():
    with open(timestamp_file, "w") as file:
        # Write the current timestamp to the file
        file.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print(datetime.datetime.now())


def rac_unit_eol_crosstab():
    execute_script("RAC_Unit_EOL_Crosstab.py")


# Schedule your tasks here
# Define common constants
DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

# Use a descriptive dictionary for tasks with specific times
TASK_SCHEDULES = {
    rac_unit_eol_crosstab: ["15:35"],
}

# Schedule tasks with specific times
for task, times in TASK_SCHEDULES.items():
    for time in times:
        schedule.every().day.at(time).do(task)


# For tasks that run every minute or hour for check ins
def log_alive_status():
    logging.info("Scheduler is still running...")


schedule.every(1).hours.do(log_alive_status)
schedule.every(1).minutes.do(test_time)

logging.info("Scheduler started and running...")

try:
    while True:
        schedule.run_pending()
        time_module.sleep(1)
except (KeyboardInterrupt, SystemExit):
    logging.info("Scheduler interrupted and gracefully shutting down...")
except Exception as e:
    logging.error("Exception occurred", exc_info=True)
