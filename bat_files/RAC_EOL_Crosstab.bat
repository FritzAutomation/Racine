@echo off
REM Windows batch script to run Python program/scripts with uv

set venv_root_dir=C:\FritzAutomation\Racine
set script_name=RAC_Scheduler.py

:: Set the title of the Command Prompt window to the script name
title Running %script_name%


cd %venv_root_dir%

echo:
echo "-------------------Starting %script_name% using uv-------------------"
echo:

:: Use uv to run the Python script within the virtual environment
uv run python %script_name%

REM Optional: If you need to specify any environment variables or configurations
REM set MY_ENV_VAR=my_value
REM uv run --env MY_ENV_VAR python %script_name%

:: Exit the batch script
exit /B 0