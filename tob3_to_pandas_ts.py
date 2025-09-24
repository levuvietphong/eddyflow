import os
import glob
import csv
import pandas as pd
import read_cs_files as cs
from natsort import natsorted
import logging
from pathlib import Path


def format_value(x, is_timestamp=False):
    """
    Format a value for CSV output.
    """
    if pd.isna(x):
        return ""
    if is_timestamp:
        # Keep only 3 decimals for milliseconds
        return f'"{pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}"'

    # Numbers: round dynamically to match your target decimals
    if isinstance(x, float):
        s = f"{x:.6f}"   # round to 6 decimals max
        s = s.rstrip('0').rstrip('.')  # remove unnecessary zeros
        return s

    if isinstance(x, int):
        return str(x)

    return str(x)


def load_data(fname):
    """
    Load data from CS files and convert to pandas DataFrame.

    Args:
        fname (str): Filename or path to the CS file to load.

    Returns:
        tuple: A tuple containing:
            - df (pd.DataFrame): DataFrame with TIMESTAMP and data columns
            - meta (list): Metadata from the CS file
    """
    bin_data, meta = cs.read_cs_files(fname)
    df = pd.DataFrame(columns = meta[2], data=None)

    if bin_data != []:
        df['TIMESTAMP'] = pd.to_datetime(bin_data[0])
        for i, col in enumerate(meta[2][1:]):
            df[col] = bin_data[i + 1]
    return df, meta


def write_full_day_data(df_day, meta, day, dst_dir, var):
    """
    Write full day data to a file in a format compatible with Eddypro engine.

    Args:
        df_day (pd.DataFrame): DataFrame containing the day's data.
        meta (list): Metadata, where meta[3] contains column headers.
        day (datetime.date or datetime.datetime): The day for which data is being written.
        dst_dir (str): Directory to write the output file.
        var (str): Variable name to include in the output filename.

    Returns:
        None
    """

     # Extract the earliest timestamp for hour and minute
    first_ts = df_day["TIMESTAMP"].min()
    hour = first_ts.hour
    minute = first_ts.minute

    # Format output filename
    hour_str = f"{hour:02d}"
    minute_str = f"{minute:02d}"

    # Set the file name in specified format for Eddypro engine
    day_str = day.strftime("%Y-%m-%d")
    file_output = os.path.join(dst_dir, f"{var}_{day_str}_{hour_str}{minute_str}.dat")

     # Write header row with quoted column names
    with open(file_output, "w", encoding="utf-8") as f:
        quoted_row = ['"{}"'.format(item) for item in meta[3]]
        f.write(",".join(quoted_row) + "\n")

    # Prepare data for writing
    formatted = df_day.copy(deep=False)
    for col in df_day.columns:
        formatted[col] = df_day[col].apply(
            lambda x: format_value(x, is_timestamp=(col == "TIMESTAMP"))
        )

     # Append data to file (no quoting, as header is already quoted)
    formatted.to_csv( file_output, index=False, quoting=csv.QUOTE_NONE, mode="a")

    # Log the last processed file name for next processing
    save_last_processed_file(dst_dir, file_output)
    print(f"Saved: {file_output}")


def save_last_processed_file(dst_dir, filename):
    """
    Log the last processed input filename to the log file.

    Args:
        dst_dir (str): Destination directory where the log file will be stored.
        filename (str): Name of the last processed file to log.

    Raises:
        OSError: If there's an issue writing to the log file.
    """
    log_file_path = os.path.join(dst_dir, "log_ts.txt")

    with open(log_file_path, "w") as log_file:
        log_file.write(f"{filename}\n")

    print(f"Logged last processed file: {filename}")


def load_last_logged_file(dst_dir):
    """
    Read the last processed file from log_ts.txt.

    Args:
        dst_dir (str or Path): Destination directory containing the log file.

    Returns:
        str or None: The filename of the last processed file, or None if
                     no log file exists, file is empty, or contains only whitespace.

    Raises:
        OSError: If there's an issue reading the log file.
    """
    log_file_path = os.path.join(dst_dir, "log_ts.txt")

    if not os.path.exists(log_file_path):
        return None  # No log file, so process all files

    with open(log_file_path, "r") as log_file:
        last_logged_file = log_file.readline().strip()

    if last_logged_file:
        print(f"Last processed file from log: {last_logged_file}")

    return last_logged_file


def filter_files_starting_from(last_logged_file, src_dir, var):
    """Filter files to include only those after the last logged file."""
    all_files = natsorted(glob.glob(os.path.join(src_dir, f"{var}*.dat")))

    if not last_logged_file:
        # No log file: return all files
        return all_files

    # Extract the last processed file number, e.g., '742' from 'ts_data742.dat'
    last_file_number = int(''.join(filter(str.isdigit, last_logged_file)))

    # Filter files with numbers greater than the last processed file number
    filtered_files = [
        f for f in all_files
        if int(''.join(filter(str.isdigit, os.path.basename(f)))) > last_file_number
    ]
    return filtered_files


def process_files_by_day(var, src_dir, dst_dir):
    """
    Process input files by day, adjusting units, writing full-day data, and logging progress.

    Args:
        var (str): Variable name to filter and process files.
        src_dir (str): Source directory containing input files.
        dst_dir (str): Destination directory for output and logs.

    Returns:
        None
    """
    # Load the last processed file from the log
    last_logged_file = load_last_logged_file(dst_dir)

    # Filter files to process based on the log information
    full_filenames = filter_files_starting_from(last_logged_file, src_dir, var)

    temp_df = pd.DataFrame()  # Temporary DataFrame to accumulate until full-day data
    meta = None
    last_saved_file = None  # Variable to track the last input file processed

    for filename in full_filenames:
        print(f"Processing: {filename}")

        # Load data
        df, file_meta = load_data(filename)

        # Adjust units for CO2 and H2O
        if file_meta[3][7] == "umol/mol":
            df["CO2"] = df["CO2"] / 44
            file_meta[3][7] = "mmol/m^3"
        elif file_meta[3][7] == "umol/m^3":
            file_meta[3][7] = "mmol/m^3"

        if file_meta[3][8] == "mmol/mol":
            df["H2O"] = df["H2O"] / 0.018
            file_meta[3][8] = "mmol/m^3"

        # Append data to temporary DataFrame
        temp_df = pd.concat([temp_df, df], ignore_index=True)

        # Add 'date' column for day grouping
        temp_df["date"] = temp_df["TIMESTAMP"].dt.date

        # Check completeness of days in accumulated data
        for day in temp_df["date"].unique():
            df_day = temp_df[temp_df["date"] == day]

            # Check if the current day's data is complete (includes last timestamp)
            last_ts = df_day["TIMESTAMP"].max()
            if last_ts.time() >= pd.to_datetime("23:59:59.900").time():
                # Write full-day data to file
                write_full_day_data(df_day, file_meta, day, dst_dir, var)

                # Update the last saved filename
                last_saved_file = os.path.basename(filename)  # Use only the file name

                # Remove processed day's data from the temporary DataFrame
                temp_df = temp_df[temp_df["date"] != day]

        # Update meta for potential metadata file writing later
        meta = file_meta if meta is None else meta

    # After processing all files, log the last saved filename
    if last_saved_file:
        save_last_processed_file(dst_dir, last_saved_file)

    # Write any remaining metadata file - this logic assumes metadata remains the same
    if meta:
        metafile = os.path.join(dst_dir, "meta.txt")
        with open(metafile, "w") as f:
            for row in meta:
                quoted_row = ['"{}"'.format(item) for item in row]
                f.write(",".join(quoted_row) + "\n")


def main():
    # Name of variable to process
    var = 'ts_data'

    # Destination directory for decoded data
    dst_dir = f'../../decoded_data/{var}'

    # Source directory for raw data
    src_dir = '../../DataLogger/CRD/'
    os.makedirs(dst_dir, exist_ok=True)

    # Run the processing function
    process_files_by_day(var, src_dir, dst_dir)


if __name__ == "__main__":
    main()
