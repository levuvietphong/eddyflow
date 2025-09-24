import os, glob
import csv
import pandas as pd
import read_cs_files as cs
import subprocess
from natsort import natsorted
from datetime import datetime

def format_timestamp(ts):
    # Format with microseconds, then strip trailing zeros
    s = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    s = s.rstrip("0").rstrip(".")  # remove extra 0s and final '.' if no decimals left
    return s


def split_30min(var, filename, dst_dir):
    with open(filename, "r", encoding="latin-1") as f:
        meta_lines = [next(f) for _ in range(4)]  # header lines
        # data_lines = f.readlines()  # all remaining rows

    # --- Step 2: Load data skipping meta ---
    df = pd.read_csv(filename, skiprows=4, header=None)
    columns =  meta_lines[1].strip().split(',')
    columns = [c.strip('"') for c in columns]
    df.columns = columns

    # List of columns to delete
    cols_to_delete = ["SonicDiag", "irga(3)", "irga_diag", "Diag77", "RSSI"]
    df = df.drop(columns=cols_to_delete)

    df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
    file_date = df["TIMESTAMP"].dt.date.iloc[0]

    for slot in range(48):
        start = pd.Timestamp(file_date) + pd.Timedelta(minutes=30 * slot)
        end = start + pd.Timedelta(minutes=30)

        # Filter rows: include start, exclude end
        chunk = df[(df["TIMESTAMP"] > start) & (df["TIMESTAMP"] <= end)]
        chunk["TIMESTAMP"] = chunk["TIMESTAMP"].apply(format_timestamp)

        if not chunk.empty:
            # Build filename
            outfile = os.path.join(
                dst_dir, f"{var}_{start.strftime('%Y-%m-%d_%H%M')}.dat"
            )
            # Write header + data (even if empty, so always 48 files)
            with open(outfile, "w", encoding="utf-8") as f:
                # f.writelines(meta_lines)
                chunk.to_csv(f, index=False, header=True, sep=',', quoting=csv.QUOTE_NONNUMERIC)


def merge_two_ascii(file1, file2, header_lines=4):
    """
    Merge file1 into file2, keeping header only from file2.
    The merged content overwrites file2.

    Args:
        file1 (str): Path to the first input file.
        file2 (str): Path to the second input file (keeps its header, final output).
        header_lines (int): Number of header lines to keep (default=1).
    """
    with open(file1, "r", encoding="utf-8", errors="replace") as f1:
        lines1 = f1.readlines()
    with open(file2, "r", encoding="utf-8", errors="replace") as f2:
        lines2 = f2.readlines()

    # Keep header from file2, then append data from file2 and file1 (skipping headers)
    merged = lines2[:header_lines] + lines2[header_lines:] + lines1[header_lines:]

    # Overwrite file2 with merged content
    with open(file2, "w", encoding="utf-8") as fout:
        fout.writelines(merged)

    print(f"Merged {file1} into {file2}")

def main():
    var = 'MetData'
    dst_dir = f'/Users/pvn/research/data/EddyFlux/SETx/TOA5/'
    src_dir = '/Users/pvn/Downloads/Download-2025-08-25'
    full_filenames = natsorted(glob.glob(os.path.join(src_dir, f'{var}*.dat')))

    for filename in full_filenames[56:]:
        print(filename)
        # _, fn = os.path.split(filename)
        _, meta = cs.read_cs_files(filename)
        dt = datetime.strptime(meta[0][-1], '%Y-%m-%d %H:%M:%S')
        yyyy = dt.year
        mm = dt.month
        dd = dt.day
        hh = dt.hour
        min = dt.minute
        print(f'{yyyy},{mm},{dd},{hh},{min}')
        outfile = os.path.join(dst_dir, f"{var}_{yyyy:04d}-{mm:02d}-{dd:02d}_0000.dat")
        if hh==0 & min==0:
            cmd = f'camp2ascii {filename} > {outfile}'
            subprocess.run(cmd, shell=True)
            # split_30min(var, outfile, dst_dir)
        else:
            tmpfile = os.path.join(dst_dir, f"{var}_{yyyy:04d}-{mm:02d}-{dd:02d}_{hh:02d}{min:02d}.dat")
            cmd = f'camp2ascii {filename} > {tmpfile}'
            subprocess.run(cmd, shell=True)
            merge_two_ascii(tmpfile, outfile, header_lines=4)
            os.remove(tmpfile)

if __name__ == "__main__":
    main()
