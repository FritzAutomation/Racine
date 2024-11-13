# Necessary imports for the routine
import os
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from pathlib import Path
from email_func_multiple import (
    main,
)

# Define constants for file locations and column names
CURRENT_DIR = Path.cwd()
PROD_DIR = Path(r"\\s1racft1\ftp\PAS\CLS")

# Column name definitions
COLUMN_NAMES = {
    "cls_unit_status.txt": [
        "Plant_code",
        "Unit_serial_number",
        "Sequence",
        "Unit_set_date",
        "Unit_end_of_line_date",
        "Unit_complete_date",
        "Assembly_line_number",
        "Assembly_line_description",
        "Zone_number",
        "Zone_description",
        "Work_station_order",
        "Work_station_number",
        "Work_station_description",
        "Employee_clock_number",
        "Employee_name",
        "Validation_date",
    ],
    "cls_req_comps.txt": [
        "Plant_code",
        "Unit_serial_number",
        "Alstar_seq",
        "Assembly_line_number",
        "Component_code",
        "Component_descp",
        "Display_order",
        "Component_serial_number",
    ],
    "cls_unit_checklist_summary.txt": [
        "Unit_serial_number",
        "Checklist_id",
        "Checklist_item_id",
        "Item_order",
        "Workstation_id",
        "Workstation_name",
        "Check_description",
        "Status",
    ],
    "cls_email_addresses.txt": ["Plant", "Report_code", "Email_address"],
}


def load_data(
    file_name: str, directory: Path = PROD_DIR, sep: str = "|", encoding: str = "latin1"
) -> pd.DataFrame:
    """
    Load a dataset from a specified file in a given directory.

    Args:
        file_name (str): Name of the file to load.
        directory (Path): Directory path where the file is located.
        sep (str): Column separator in the file.
        encoding (str): File encoding type.

    Returns:
        pd.DataFrame: Loaded data as a DataFrame. Returns an empty DataFrame if the file is not found.
    """
    file_path = directory / file_name
    col_names = COLUMN_NAMES.get(file_name, None)

    try:
        return pd.read_csv(
            file_path, sep=sep, header=None, names=col_names, encoding=encoding
        )
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return pd.DataFrame()  # Return an empty DataFrame in case of error


# Load all datasets
data_files = [
    "cls_unit_status.txt",
    "cls_req_comps.txt",
    "cls_unit_checklist_summary.txt",
    "cls_email_addresses.txt",
]
data_frames = {file_name: load_data(file_name) for file_name in data_files}

# Access each dataset by its file name
unit_status = data_frames["cls_unit_status.txt"]
req_comps = data_frames["cls_req_comps.txt"]
unit_checklist_summary = data_frames["cls_unit_checklist_summary.txt"]
email_addresses = data_frames["cls_email_addresses.txt"]

# Check if each dataset is empty and print the result
datasets = {
    "unit_status": unit_status,
    "req_comps": req_comps,
    "unit_checklist_summary": unit_checklist_summary,
    "email_addresses": email_addresses,
}

for name, df in datasets.items():
    if df.empty:
        print(f"{name} is empty.")
    else:
        print(f"{name} contains data with {len(df)} rows.")

# Filtering on the report code 'UNITEOL' for this report
email_addresses = email_addresses.loc[
    email_addresses["Report_code"] == "UNITEOL"
].dropna()

# Add 'First_Name' column, select and reorder columns, then export directly
email_addresses = email_addresses.assign(
    First_Name=email_addresses["Email_address"].str.split(".", n=1).str[0]
)[["First_Name", "Email_address"]]

# Export to a txt file with the specified format
email_addresses.to_csv(
    "mycontacts_rac_unit_eol_crosstab.txt", index=False, sep="\t", header=None
)
# Set of work station descriptions for faster filtering
work_station_descriptions = {
    "TRANNY LOAD",
    "AXLE OIL FILL",
    "CHASSIS OVERHEAD",
    "VERIFY ROPS PLATE",
    "CAB COMPLETE",
    "QAA - Inside man",
    "QAA - Outside man",
    "QAA - Seat safety switch",
    "QAA - Bleed Trailer Brake",
    "CAB LINE TEST",
    "CAB POWER UP",
    "QAA - Brake assembly",
    "QAA - MFD system",
    "QAA - PTO sys & hydr",
    "QAA - Hitch sys & hydr",
    "QAA - Remote system",
    "QAA - Diff lock",
    "QAA - Trans drive system",
    "QAA - Verify PIN",
    "QAA-BACKUP ALARM FUNCTION",
    "COC (Cert. of Conformity)",
    "QAA - Engine Oil Check",
    "QAA Emergency Brake Test",
    "Hydraulic Cycle Test",
    "UNIT BUILT",
    "FINAL Quality Gate 2",
    "Hood & Model Decals",
    "QAA - Susp Axle Calibrtn",
    "Trans Oil Level Check",
    "Wash Tractor Complete",
    "CAB WATER TEST",
}

# Filter and drop duplicates in a single step
station_filter = unit_status.loc[
    unit_status["Work_station_description"].isin(work_station_descriptions),
    ["Work_station_description"],
].drop_duplicates()

# Inner join to get the filtered unit status based on work station descriptions
filtered_unit_status = pd.merge(
    unit_status, station_filter, on="Work_station_description", how="inner"
)

# Format 'Unit_end_of_line_date' column to a datetime object, stripping whitespace
filtered_unit_status["Unit_end_of_line_date_b"] = pd.to_datetime(
    filtered_unit_status["Unit_end_of_line_date"].str.strip(), errors="coerce"
).dt.date

# Define dictionary of date references for easy access and printing
date_refs = {
    "yesterday": date.today() - timedelta(days=1),
    "today": date.today(),
    "tomorrow": date.today() + timedelta(days=1),
}

# Print date references for verification
for name, date_val in date_refs.items():
    print(f"{name.capitalize()} : {date_val}")

# Filter for units that came off line on today's date
filtered_unit_status = filtered_unit_status[
    filtered_unit_status["Unit_end_of_line_date_b"] == date_refs["today"]
]

# Get a unique list of 'Unit_serial_number' values from filtered_unit_status
unique_unit_serial_numbers = (
    filtered_unit_status["Unit_serial_number"].unique().tolist()
)

# Example of using the unique list to filter another DataFrame
req_comps = req_comps[req_comps["Unit_serial_number"].isin(unique_unit_serial_numbers)]
# Example of using the unique list to filter another DataFrame
unit_checklist_summary = unit_checklist_summary[
    unit_checklist_summary["Unit_serial_number"].isin(unique_unit_serial_numbers)
]

# sort the units by sequence number
filtered_unit_status.sort_values(by="Sequence", ascending=True, inplace=True)

# Strip whitespace and convert 'Validation_date' to datetime in a single step
filtered_unit_status["Validation_date"] = pd.to_datetime(
    filtered_unit_status["Validation_date"].str.strip(), errors="coerce"
)

# Create a copy of the DataFrame and initialize the invalid rows DataFrame
modified_df = filtered_unit_status.copy()


# Extract the validation date for Work_station_order 182 for each Unit_serial_number group
modified_df["Date_182"] = pd.to_datetime(
    modified_df.groupby("Unit_serial_number")["Validation_date"].transform(
        lambda x: x[modified_df["Work_station_order"] == 182].max()
    )
)

# Check that both columns are of datetime type before comparison
if pd.api.types.is_datetime64_any_dtype(
    modified_df["Validation_date"]
) and pd.api.types.is_datetime64_any_dtype(modified_df["Date_182"]):
    invalid_rows_df = modified_df[
        (modified_df["Work_station_order"] < 182)
        & (modified_df["Validation_date"] > modified_df["Date_182"])
    ]
else:
    print("One of the columns is not in datetime format.")

# Set invalid entries to NaN for specified columns directly in modified_df
modified_df.loc[
    invalid_rows_df.index, ["Employee_clock_number", "Employee_name", "Validation_date"]
] = [np.nan, np.nan, pd.NaT]

# Drop the helper column Date_182 if it's no longer needed
modified_df.drop(columns=["Date_182"], inplace=True)

# List of columns to potentially drop
columns_to_drop = [
    "Plant_code",
    "Assembly_line_number",
    "Assembly_line_description",
    "Zone_number",
    "Work_station_order",
    "Work_station_number",
    "Unit_end_of_line_date_b",
]

# Drop columns directly, ignoring any that don't exist
invalid_rows_df.drop(columns=columns_to_drop, inplace=True, errors="ignore")

# Update 'Employee_clock_number', 'Employee_name', and 'Validation_date' for invalid rows in a single step
cols_to_update = ["Employee_clock_number", "Employee_name", "Validation_date"]
filtered_unit_status.loc[invalid_rows_df.index, cols_to_update] = modified_df.reindex(
    invalid_rows_df.index
)[cols_to_update]

# Reset the index to get back to the original structure
filtered_unit_status.reset_index(drop=True, inplace=True)

# Convert 'Component_serial_number' to string, strip spaces, and calculate length in one step
req_comps["Component_serial_number"] = (
    req_comps["Component_serial_number"].astype(str).str.strip()
)
req_comps["Component_serial_number_len"] = req_comps[
    "Component_serial_number"
].str.len()

# Update 'Display_order' based on 'Component_descp' values using a dictionary
display_order_mapping = {
    "Novatel": "999",
    "CAN Diagnostics": "998",
    "Davachi": "997",
    "ESOM Vehicle Snapshot": "996",
}
# Update 'Display_order' only where 'Component_descp' matches the mapping keys
for descp, display_order in display_order_mapping.items():
    req_comps.loc[req_comps["Component_descp"] == descp, "Display_order"] = (
        display_order
    )

# Assign 'Yes' where 'Component_serial_number_len' is greater than 0
req_comps["Test"] = np.where(
    req_comps.get("Component_serial_number_len", 0) > 0, "Yes", None
)

# Assign 'Yes' where 'Status' equals 1 in 'unit_checklist_summary'
unit_checklist_summary["Test_Status"] = np.where(
    unit_checklist_summary.get("Status", 0) == 1, "Yes", None
)

# create two dataframes from the original filtered unit status to break out the cabs and the tractors on different tabs
cab_unit_status = filtered_unit_status[
    ~filtered_unit_status["Unit_serial_number"].str.startswith("Z")
]

filtered_unit_status = filtered_unit_status[
    filtered_unit_status["Unit_serial_number"].str.startswith("Z")
]
cab_req_comps = req_comps[~req_comps["Unit_serial_number"].str.startswith("Z")]
unit_req_comps = req_comps[req_comps["Unit_serial_number"].str.startswith("Z")]
cab_checklist_summary = unit_checklist_summary[
    ~unit_checklist_summary["Unit_serial_number"].str.startswith("Z")
]
unit_checklist_summary = unit_checklist_summary[
    unit_checklist_summary["Unit_serial_number"].str.startswith("Z")
]

# slice the unit serial number to only have the values after the 5th character. We do this so that it will sort the columns index in the dataframe correctly
filtered_unit_status["Unit_serial_number"] = (
    filtered_unit_status.Unit_serial_number.str[5:]
)
cab_unit_status["Unit_serial_number"] = cab_unit_status.Unit_serial_number.str[5:]

cab_req_comps["Unit_serial_number"] = cab_req_comps.Unit_serial_number.str[5:]
unit_req_comps["Unit_serial_number"] = req_comps.Unit_serial_number.str[5:]
cab_checklist_summary["Unit_serial_number"] = (
    cab_checklist_summary.Unit_serial_number.str[5:]
)
unit_checklist_summary["Unit_serial_number"] = (
    unit_checklist_summary.Unit_serial_number.str[5:]
)

# create a list of checks for cabs according to the plants specificiations
cab_options = [
    "CAB OPERATOR 1",
    "CAB OPERATOR 2",
    "CAB OPERATOR 7",
    "CAB OPERATOR 8",
    "CAB OPERATOR 9",
    "CAB OPERATOR 10",
    "CAB OPERATOR 11",
    "CAB OPERATOR 12",
    "CAB OPERATOR 13",
    "CAB OPERATOR 14",
    "CAB OPERATOR 15",
    "CAB OPERATOR 16",
    "CAB OPERATOR 18",
    "CAB OPERATOR 21",
    "CAB SEAT/ARU OP. 1",
    "CH26 FIREWALL SUB",
    "CAB LINE TEST",
    "CAB POWER UP",
    "CAB COMPLETE",
]

# filter the data by the list created above
cab_checklist_summary = cab_checklist_summary[
    cab_checklist_summary["Check_description"].isin(cab_options)
]

# create a list of checks for tractors according to the plants specificiations
tractor_options = [
    "CAB OPERATOR 1",
    "CAB OPERATOR 2",
    "CAB OPERATOR 7",
    "CAB OPERATOR 8",
    "CAB OPERATOR 9",
    "CAB OPERATOR 10",
    "CAB OPERATOR 11",
    "CAB OPERATOR 12",
    "CAB OPERATOR 13",
    "CAB OPERATOR 14",
    "CAB OPERATOR 15",
    "CAB OPERATOR 16",
    "CAB OPERATOR 18",
    "CAB OPERATOR 21",
    "CAB SEAT/ARU OP. 1",
    "CH26 FIREWALL SUB",
    "CAB LINE TEST",
    "CAB POWER UP",
    "CAB COMPLETE",
    "TRANNY LOAD",
    "ENGINE SUB",
    "CHASSIS STATION 7",
    "CHASSIS STATION 9",
    "Masking",
    "FINAL LINE STATION 02",
    "FINAL LINE STATION 03",
    "FINAL LINE STATION 04",
    "FINAL LINE STATION 07",
    "FINAL LINE STATION 08",
    "FINAL LINE STATION 11",
    "FINAL LINE STATION 11.5",
    "FINAL LINE STATION 12",
    "FINAL LINE STATION 13",
    "FINAL LINE STATION 14",
    "FINAL LINE STATION 15.5",
    "FINAL LINE STATION 16",
    "FINAL LINE STATION 17",
    "FINAL LINE STATION 17.5",
    "FINAL LINE STATION 3.5",
    "QAA - Engine Oil Check",
    "QAA - Inside man",
    "QAA - Outside man",
    "FINAL LINE RADIATOR SUB",
    "HOOD SUB",
    "FTQ - Mark, Torque, Mark",
    "FINAL Quality Gate 2",
    "Wash Tractor Complete",
]

# filter the data by the list created above
unit_checklist_summary = unit_checklist_summary[
    unit_checklist_summary["Check_description"].isin(tractor_options)
]
# Define Item_order mappings
item_order_mapping = {
    "CAB OPERATOR 1": 1,
    "CAB OPERATOR 2": 2,
    "CAB OPERATOR 7": 3,
    "CAB OPERATOR 8": 4,
    "CAB OPERATOR 9": 5,
    "CAB OPERATOR 10": 6,
    "CAB OPERATOR 11": 7,
    "CAB OPERATOR 12": 8,
    "CAB OPERATOR 13": 9,
    "CAB OPERATOR 14": 10,
    "CAB OPERATOR 15": 11,
    "CAB OPERATOR 16": 12,
    "CAB OPERATOR 18": 13,
    "CAB OPERATOR 21": 14,
    "CAB SEAT/ARU OP. 1": 15,
    "CH26 FIREWALL SUB": 16,
    "CAB LINE TEST": 17,
    "CAB POWER UP": 18,
    "CAB COMPLETE": 19,
    "TRANNY LOAD": 20,
    "ENGINE SUB": 21,
    "CHASSIS STATION 7": 22,
    "CHASSIS STATION 9": 23,
    "Masking": 24,
    "FINAL LINE STATION 02": 25,
    "FINAL LINE STATION 03": 26,
    "FINAL LINE STATION 04": 27,
    "FINAL LINE STATION 07": 28,
    "FINAL LINE STATION 08": 29,
    "FINAL LINE STATION 11": 30,
    "FINAL LINE STATION 11.5": 31,
    "FINAL LINE STATION 12": 32,
    "FINAL LINE STATION 13": 33,
    "FINAL LINE STATION 14": 34,
    "FINAL LINE STATION 15.5": 35,
    "FINAL LINE STATION 16": 36,
    "FINAL LINE STATION 17": 37,
    "FINAL LINE STATION 17.5": 38,
    "FINAL LINE STATION 3.5": 39,
    "QAA - Engine Oil Check": 40,
    "QAA - Inside man": 41,
    "QAA - Outside man": 42,
    "FINAL LINE RADIATOR SUB": 43,
    "HOOD SUB": 44,
    "FTQ - Mark, Torque, Mark": 45,
    "FINAL Quality Gate 2": 46,
    "Wash Tractor Complete": 47,
}

# Apply Item_order mappings to both cab_checklist_summary and unit_checklist_summary
cab_checklist_summary["Item_order"] = (
    cab_checklist_summary["Check_description"]
    .map(item_order_mapping)
    .fillna(cab_checklist_summary["Item_order"])
)
unit_checklist_summary["Item_order"] = (
    unit_checklist_summary["Check_description"]
    .map(item_order_mapping)
    .fillna(unit_checklist_summary["Item_order"])
)

# create a crosstab with the merged dataframe
crosstab_unit_status = pd.crosstab(
    index=[
        filtered_unit_status.Work_station_order,
        filtered_unit_status.Work_station_description,
    ],
    columns=filtered_unit_status.Unit_serial_number,
    values=filtered_unit_status.Employee_name,
    aggfunc="count",
)
crosstab_cab_status = pd.crosstab(
    index=[
        cab_unit_status.Work_station_order,
        cab_unit_status.Work_station_description,
    ],
    columns=cab_unit_status.Unit_serial_number,
    values=cab_unit_status.Employee_name,
    aggfunc="count",
)

# Check if 'Test' column exists in the DataFrame
if "Test" in cab_req_comps.columns:
    print("Column 'Test' is present. Proceeding with cab_req_comps crosstab.")
    crosstab_cab_req_comps = pd.crosstab(
        index=[cab_req_comps.Display_order, cab_req_comps.Component_descp],
        columns=cab_req_comps.Unit_serial_number,
        values=cab_req_comps.Test,
        aggfunc="count",
    )
else:
    print("Column 'Test' is not present. Skipping crosstab.")

# Check if 'Test' column exists in the DataFrame
if "Test" in unit_req_comps.columns:
    print("Column 'Test' is present. Proceeding with unit_req_comps crosstab.")
    crosstab_unit_req_comps = pd.crosstab(
        index=[unit_req_comps.Display_order, unit_req_comps.Component_descp],
        columns=unit_req_comps.Unit_serial_number,
        values=unit_req_comps.Test,
        aggfunc="count",
    )
else:
    print("Column 'Test' is not present. Skipping crosstab.")

# Check if 'Test_Status' column exists in cab_checklist_summary
if "Test_Status" in cab_checklist_summary.columns:
    print(
        "Column 'Test_Status' is present in cab_checklist_summary. Proceeding with crosstab."
    )
    cab_checklist_summary_crosstab = pd.crosstab(
        index=[
            cab_checklist_summary.Item_order,
            cab_checklist_summary.Check_description,
        ],
        columns=cab_checklist_summary.Unit_serial_number,
        values=cab_checklist_summary.Test_Status,
        aggfunc="count",
    )
else:
    print(
        "Column 'Test_Status' is not present in cab_checklist_summary. Skipping crosstab."
    )

# Check if 'Test_Status' column exists in unit_checklist_summary
if "Test_Status" in unit_checklist_summary.columns:
    print(
        "Column 'Test_Status' is present in unit_checklist_summary. Proceeding with crosstab."
    )
    unit_checklist_summary = pd.crosstab(
        index=[
            unit_checklist_summary.Item_order,
            unit_checklist_summary.Check_description,
        ],
        columns=unit_checklist_summary.Unit_serial_number,
        values=unit_checklist_summary.Test_Status,
        aggfunc="count",
    )
else:
    print(
        "Column 'Test_Status' is not present in unit_checklist_summary. Skipping crosstab."
    )

# Replace NaN values with 1 if the variable exists
if "crosstab_unit_status" in locals():
    crosstab_unit_status = crosstab_unit_status.replace(np.nan, 1)

if "crosstab_cab_status" in locals():
    crosstab_cab_status = crosstab_cab_status.replace(np.nan, 1)

if "crosstab_cab_req_comps" in locals():
    crosstab_cab_req_comps = crosstab_cab_req_comps.replace(np.nan, 1)

if "crosstab_unit_req_comps" in locals():
    crosstab_unit_req_comps = crosstab_unit_req_comps.replace(np.nan, 1)

if "cab_checklist_summary" in locals():
    cab_checklist_summary = cab_checklist_summary.replace(np.nan, 1)

if "unit_checklist_summary" in locals():
    unit_checklist_summary = unit_checklist_summary.replace(np.nan, 1)

# Specific value to count
value_to_count = 1.0

# Add a new column that counts the occurrences of the specific value in each row
crosstab_unit_status["Count"] = crosstab_unit_status.apply(
    lambda row: (row == value_to_count).sum(), axis=1
)

# Calculate the TotalCount excluding the "Count" column
crosstab_unit_status["TotalCount"] = crosstab_unit_status.drop(columns=["Count"]).apply(
    lambda row: row.count(), axis=1
)

# Calculate the percentage and assign to a new column
crosstab_unit_status["%"] = (
    ((crosstab_unit_status["Count"] / crosstab_unit_status["TotalCount"]) * 100)
    .round()
    .astype(int)
)

# Extract the list of column names
cols = crosstab_unit_status.columns.tolist()

# Remove the "%" column from its current position
cols.remove("%")

# Insert the "%" column to be the third column
cols.insert(0, "%")

# Rearrange the DataFrame columns using the reordered list
crosstab_unit_status = crosstab_unit_status[cols]

# Drop the "Count" and "TotalCount" columns
crosstab_unit_status = crosstab_unit_status.drop(columns=["Count", "TotalCount"])


# change directory to the Reports_PD folder
current_dir = os.getcwd()
# desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
os.chdir(current_dir + "\Reports")

# create a todays date/time variable to use in naming the file
todays_date = str(datetime.now().strftime("%Y-%m-%d_%H_%M")) + ".xlsx"

# List of DataFrame variables to check
dataframes = [
    "crosstab_unit_status",
    "crosstab_cab_status",
    "crosstab_unit_req_comps",
    "crosstab_cab_req_comps",
    "cab_checklist_summary",
    "unit_checklist_summary",
    "invalid_rows_df",
]

# Check each DataFrame and print whether it exists and is not empty
for df_name in dataframes:
    if df_name in locals():
        if not locals()[df_name].empty:
            print(f"{df_name}: True (exists and is not empty)")
        else:
            print(f"{df_name}: False (exists but is empty)")
    else:
        print(f"{df_name}: False (does not exist)")

# Check if any DataFrame exists and is not empty
run_main = (
    ("crosstab_unit_status" in locals() and crosstab_unit_status.empty is False)
    or ("crosstab_cab_status" in locals() and crosstab_cab_status.empty is False)
    or (
        "crosstab_unit_req_comps" in locals() and crosstab_unit_req_comps.empty is False
    )
    or ("crosstab_cab_req_comps" in locals() and crosstab_cab_req_comps.empty is False)
    or ("cab_checklist_summary" in locals() and cab_checklist_summary.empty is False)
    or ("unit_checklist_summary" in locals() and unit_checklist_summary.empty is False)
    or ("invalid_rows_df" in locals() and invalid_rows_df.empty is False)
)

# Only create the writer and save the file if there is data to write
if run_main:
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter("RAC_Unit_EOL_Report_" + todays_date, engine="xlsxwriter")

    # Check if the variable exists and is not empty
    if "crosstab_unit_status" in locals() and crosstab_unit_status.empty is False:
        crosstab_unit_status.to_excel(writer, sheet_name="Tractor", startrow=0)

    if "crosstab_cab_status" in locals() and crosstab_cab_status.empty is False:
        crosstab_cab_status.to_excel(writer, sheet_name="Cab", startrow=0)

    if "crosstab_unit_req_comps" in locals() and crosstab_unit_req_comps.empty is False:
        crosstab_unit_req_comps.to_excel(
            writer, sheet_name="Tractor_Req_Comps", startrow=0
        )

    if "crosstab_cab_req_comps" in locals() and crosstab_cab_req_comps.empty is False:
        crosstab_cab_req_comps.to_excel(writer, sheet_name="Cab_Req_Comps", startrow=0)

    if "cab_checklist_summary" in locals() and cab_checklist_summary.empty is False:
        cab_checklist_summary.to_excel(writer, sheet_name="Cab_Checklist", startrow=0)

    if "unit_checklist_summary" in locals() and unit_checklist_summary.empty is False:
        unit_checklist_summary.to_excel(
            writer, sheet_name="Tractor_Checklist", startrow=0
        )

    if "invalid_rows_df" in locals() and invalid_rows_df.empty is False:
        invalid_rows_df.to_excel(writer, sheet_name="Late_SignOffs", startrow=0)

    # Get the xlsxwriter objects from the dataframe writer object.
    workbook = writer.book

    # Check if the variable exists and is not empty before assigning the worksheet.
    if "crosstab_unit_status" in locals() and crosstab_unit_status.empty is False:
        worksheet1 = writer.sheets["Tractor"]

    if "crosstab_cab_status" in locals() and crosstab_cab_status.empty is False:
        worksheet2 = writer.sheets["Cab"]

    if "crosstab_unit_req_comps" in locals() and crosstab_unit_req_comps.empty is False:
        worksheet3 = writer.sheets["Tractor_Req_Comps"]

    if "crosstab_cab_req_comps" in locals() and crosstab_cab_req_comps.empty is False:
        worksheet4 = writer.sheets["Cab_Req_Comps"]

    if "cab_checklist_summary" in locals() and cab_checklist_summary.empty is False:
        worksheet5 = writer.sheets["Cab_Checklist"]

    if "unit_checklist_summary" in locals() and unit_checklist_summary.empty is False:
        worksheet6 = writer.sheets["Tractor_Checklist"]

    if "invalid_rows_df" in locals() and invalid_rows_df.empty is False:
        worksheet7 = writer.sheets["Late_SignOffs"]

    # Initialize variables to default values in case the DataFrames don't exist
    max_row, max_col = 0, 0
    max_row_1, max_col_1 = 0, 0
    max_row_2, max_col_2 = 0, 0
    max_row_3, max_col_3 = 0, 0
    max_row_4, max_col_4 = 0, 0
    max_row_5, max_col_5 = 0, 0
    max_row_6, max_col_6 = 0, 0
    max_row_7, max_col_7 = 0, 0

    # Check if the variable exists and is not empty, then get its shape
    if "crosstab_unit_status" in locals() and crosstab_unit_status.empty is False:
        (max_row, max_col) = crosstab_unit_status.shape

    if "crosstab_cab_status" in locals() and crosstab_cab_status.empty is False:
        (max_row_1, max_col_1) = crosstab_cab_status.shape

    if "crosstab_cab_req_comps" in locals() and crosstab_cab_req_comps.empty is False:
        (max_row_2, max_col_2) = crosstab_cab_req_comps.shape

    if "crosstab_unit_req_comps" in locals() and crosstab_unit_req_comps.empty is False:
        (max_row_3, max_col_3) = crosstab_unit_req_comps.shape

    # Ensure the variable is not rechecked
    if "crosstab_cab_req_comps" in locals() and crosstab_cab_req_comps.empty is False:
        (max_row_4, max_col_4) = crosstab_cab_req_comps.shape

    if "cab_checklist_summary" in locals() and cab_checklist_summary.empty is False:
        (max_row_5, max_col_5) = cab_checklist_summary.shape

    if "unit_checklist_summary" in locals() and unit_checklist_summary.empty is False:
        (max_row_6, max_col_6) = unit_checklist_summary.shape

    if "invalid_rows_df" in locals() and invalid_rows_df.empty is False:
        (max_row_7, max_col_7) = invalid_rows_df.shape

    # conditional formatting
    format2 = workbook.add_format({"bg_color": "green"})
    format3 = workbook.add_format({"bg_color": "red", "font_color": "white"})
    format4 = workbook.add_format({"bg_color": "orange"})
    cell_format = workbook.add_format({"align": "center", "bold": False})
    cell_format1 = workbook.add_format({"align": "left"})
    cell_format2 = workbook.add_format({"num_format": "mm/dd/yyyy"})
    cell_format3 = workbook.add_format({"align": "center", "font_color": "white"})
    cell_format4 = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "bg_color": "#D9D9D9",
            "border": 1,
        }
    )
    cell_format5 = workbook.add_format(
        {
            "bold": True,
            "align": "center",
            "bg_color": "purple",
            "font_color": "white",
        }
    )

    # Check if the DataFrame exists and is not empty, then apply formatting
    if (
        "crosstab_unit_status" in locals()
        and crosstab_unit_status.empty is False
        and "worksheet1" in locals()
    ):
        worksheet1.set_column("B:B", 30, cell_format)
        worksheet1.set_column("C:IA", 5, cell_format)
        worksheet1.write_string("B1", "Work_station_description", cell_format4)

    if (
        "crosstab_cab_status" in locals()
        and crosstab_cab_status.empty is False
        and "worksheet2" in locals()
    ):
        worksheet2.set_column("B:B", 30, cell_format)
        worksheet2.set_column("C:IA", 5, cell_format)
        worksheet2.write_string("B1", "Work_station_description", cell_format4)

    if (
        "crosstab_unit_req_comps" in locals()
        and crosstab_unit_req_comps.empty is False
        and "worksheet3" in locals()
    ):
        worksheet3.set_column("B:B", 30, cell_format)
        worksheet3.set_column("C:IA", 5, cell_format)
        worksheet3.write_string("B1", "Work_station_description", cell_format4)

    if (
        "crosstab_cab_req_comps" in locals()
        and crosstab_cab_req_comps.empty is False
        and "worksheet4" in locals()
    ):
        worksheet4.set_column("B:B", 30, cell_format)
        worksheet4.set_column("C:IA", 5, cell_format)
        worksheet4.write_string("B1", "Work_station_description", cell_format4)

    if (
        "cab_checklist_summary" in locals()
        and cab_checklist_summary.empty is False
        and "worksheet5" in locals()
    ):
        worksheet5.set_column("B:B", 30, cell_format)
        worksheet5.set_column("C:IA", 5, cell_format)
        worksheet5.write_string("B1", "Check_description", cell_format4)

    if (
        "unit_checklist_summary" in locals()
        and unit_checklist_summary.empty is False
        and "worksheet6" in locals()
    ):
        worksheet6.set_column("B:B", 30, cell_format)
        worksheet6.set_column("C:IA", 5, cell_format)
        worksheet6.write_string("B1", "Check_description", cell_format4)

    if (
        "invalid_rows_df" in locals()
        and invalid_rows_df.empty is False
        and "worksheet7" in locals()
    ):
        worksheet7.set_column("B:B", 19, cell_format)
        worksheet7.set_column("C:C", 9, cell_format)
        worksheet7.set_column("D:D", 22, cell_format)
        worksheet7.set_column("E:E", 22, cell_format)
        worksheet7.set_column("F:F", 22, cell_format)
        worksheet7.set_column("G:G", 17, cell_format)
        worksheet7.set_column("H:H", 24, cell_format)
        worksheet7.set_column("I:I", 24, cell_format)
        worksheet7.set_column("J:J", 16, cell_format)
        worksheet7.set_column("K:K", 18, cell_format)
        worksheet7.set_column("L:L", 18, cell_format)

    def get_crosstab(dataframe, worksheet):
        # manually adding the data and formatting for the dataframe
        row_idx, col_idx = dataframe.shape
        for r in range(row_idx):
            if r == 1:
                header_format = workbook.add_format(
                    {
                        "bold": True,
                        "bottom": 2,
                        "align": "center",
                        "text_wrap": True,
                        "bg_color": "#D9D9D9",
                        "border": 1,
                    }
                )

                for col_num, data in enumerate(dataframe.columns.values):
                    worksheet.write(0, col_num + 2, data, header_format)
            for c in range(col_idx):
                worksheet.write(
                    r + 1,
                    c + 2,
                    dataframe.values[r, c],
                    workbook.add_format(
                        {"border": 1, "align": "center"},
                    ),
                )
                if r == 1 and c == 1:
                    worksheet.conditional_format(
                        1,
                        1,
                        row_idx - 0,
                        col_idx - -1,
                        {
                            "type": "cell",
                            "criteria": "=",
                            "value": 0,
                            "format": format3,
                        },
                    )

    # Check if the DataFrame exists and is not empty, then apply the function
    if (
        "crosstab_unit_status" in locals()
        and crosstab_unit_status.empty is False
        and "worksheet1" in locals()
    ):
        get_crosstab(crosstab_unit_status, worksheet1)

    if (
        "crosstab_cab_status" in locals()
        and crosstab_cab_status.empty is False
        and "worksheet2" in locals()
    ):
        get_crosstab(crosstab_cab_status, worksheet2)

    if (
        "crosstab_unit_req_comps" in locals()
        and crosstab_unit_req_comps.empty is False
        and "worksheet3" in locals()
    ):
        get_crosstab(crosstab_unit_req_comps, worksheet3)

    if (
        "crosstab_cab_req_comps" in locals()
        and crosstab_cab_req_comps.empty is False
        and "worksheet4" in locals()
    ):
        get_crosstab(crosstab_cab_req_comps, worksheet4)

    if (
        "cab_checklist_summary" in locals()
        and cab_checklist_summary.empty is False
        and "worksheet5" in locals()
    ):
        get_crosstab(cab_checklist_summary, worksheet5)

    if (
        "unit_checklist_summary" in locals()
        and unit_checklist_summary.empty is False
        and "worksheet6" in locals()
    ):
        get_crosstab(unit_checklist_summary, worksheet6)

    # Check if the DataFrame exists and is not empty before applying operations on the worksheet
    if (
        "crosstab_unit_status" in locals()
        and crosstab_unit_status.empty is False
        and "worksheet1" in locals()
    ):
        worksheet1.set_column("A:A", None, None, {"hidden": True})
        worksheet1.hide_gridlines(2)
        worksheet1.set_landscape()
        worksheet1.center_horizontally()
        worksheet1.center_vertically()
        worksheet1.print_area("B1:Q27")  # Cells B1 to Q27.
        worksheet1.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet1.set_print_scale(105)
        worksheet1.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet1.set_footer("&L&F&C&D&R&P")

    if (
        "crosstab_cab_status" in locals()
        and crosstab_cab_status.empty is False
        and "worksheet2" in locals()
    ):
        worksheet2.set_column("A:A", None, None, {"hidden": True})
        worksheet2.hide_gridlines(2)
        worksheet2.set_landscape()
        worksheet2.center_horizontally()
        worksheet2.center_vertically()
        worksheet2.print_area("B1:Q27")  # Cells B1 to Q27.
        worksheet2.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet2.set_print_scale(105)
        worksheet2.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet2.set_footer("&L&F&C&D&R&P")

    if (
        "crosstab_unit_req_comps" in locals()
        and crosstab_unit_req_comps.empty is False
        and "worksheet3" in locals()
    ):
        worksheet3.set_column("A:A", None, None, {"hidden": True})
        worksheet3.hide_gridlines(2)
        worksheet3.set_portrait()
        worksheet3.center_horizontally()
        worksheet3.center_vertically()
        worksheet3.print_area("B1:Q56")  # Cells B1 to Q27.
        worksheet3.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet3.set_print_scale(80)
        worksheet3.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet3.set_footer("&L&F&C&D&R&P")

    if (
        "crosstab_cab_req_comps" in locals()
        and crosstab_cab_req_comps.empty is False
        and "worksheet4" in locals()
    ):
        worksheet4.set_column("A:A", None, None, {"hidden": True})
        worksheet4.hide_gridlines(2)
        worksheet4.set_portrait()
        worksheet4.center_horizontally()
        worksheet4.center_vertically()
        worksheet4.print_area("B1:Q56")  # Cells B1 to Q27.
        worksheet4.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet4.set_print_scale(80)
        worksheet4.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet4.set_footer("&L&F&C&D&R&P")

    if (
        "cab_checklist_summary" in locals()
        and cab_checklist_summary.empty is False
        and "worksheet5" in locals()
    ):
        worksheet5.set_column("A:A", None, None, {"hidden": True})
        worksheet5.hide_gridlines(2)
        worksheet5.set_portrait()
        worksheet5.center_horizontally()
        worksheet5.center_vertically()
        worksheet5.print_area("B1:Q56")  # Cells B1 to Q27.
        worksheet5.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet5.set_print_scale(80)
        worksheet5.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet5.set_footer("&L&F&C&D&R&P")

    if (
        "unit_checklist_summary" in locals()
        and unit_checklist_summary.empty is False
        and "worksheet6" in locals()
    ):
        worksheet6.set_column("A:A", None, None, {"hidden": True})
        worksheet6.hide_gridlines(2)
        worksheet6.set_portrait()
        worksheet6.center_horizontally()
        worksheet6.center_vertically()
        worksheet6.print_area("B1:Q56")  # Cells B1 to Q27.
        worksheet6.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet6.set_print_scale(80)
        worksheet6.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet6.set_footer("&L&F&C&D&R&P")

    if (
        "invalid_rows_df" in locals()
        and invalid_rows_df.empty is False
        and "worksheet7" in locals()
    ):
        worksheet7.set_column("A:A", None, None, {"hidden": True})
        worksheet7.hide_gridlines(2)
        worksheet7.set_landscape()
        worksheet7.center_horizontally()
        worksheet7.center_vertically()
        worksheet7.print_area("B1:K20")  # Cells B1 to Q27.
        worksheet7.set_header(
            "&L&G",
            {
                "image_left": r"C:\Users\A0313FC\OneDrive - CNH Industrial\Desktop\Python\Wichita\cnh_thumbnail.png"
            },
        )
        worksheet7.set_print_scale(65)
        worksheet7.set_margins(left=0.45, right=0.45, top=0.75, bottom=0.25)
        worksheet7.set_footer("&L&F&C&D&R&P")

    # Close the Pandas Excel writer and output the Excel file.
    writer.close()

# change the directory back to the main past dues folder
os.chdir(current_dir)

# calling email function from email_func_multiple.py
# if run_main:
#     main(
#         "mycontacts_rac_test.txt",
#         "templates\\message_rac_unit_eol_crosstab.html",
#         "Racine Unit End Of Line Report",
#         "RAC_Unit_EOL_Report_",
#         "\\Reports\\",
#         r"\\s1racft1\ftp\PAS\CLS\cls_unit_checklist_details.txt",
#     )  # used for testing
# else:
#     print("No DataFrames exist or all are empty. Skipping main function.")

# If any DataFrame exists and is not empty, run the main function
if run_main:
    main(
        "mycontacts_rac_unit_eol_crosstab.txt",
        "templates\\message_rac_unit_eol_crosstab.html",
        "Racine Unit End Of Line Report",
        "RAC_Unit_EOL_Report_",
        "\\Racine\\Reports\\",
        r"\\s1racft1\ftp\PAS\CLS\cls_unit_checklist_details.txt",
    )  # used for production
else:
    print("No DataFrames exist or all are empty. Skipping main function.")
