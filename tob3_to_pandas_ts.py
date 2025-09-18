import os, glob
import csv
import pandas as pd
import read_cs_files as cs
import subprocess
from natsort import natsorted
from datetime import datetime


def format_value(x, is_timestamp=False):
    if pd.isna(x):
        return ""
    if is_timestamp:
        # Keep only 3 decimals for milliseconds
        return f'"{pd.to_datetime(x).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}"'
    # Numbers: round dynamically to match your target decimals
    if isinstance(x, float):
        # Example mapping based on your row
        # Adjust as needed per column
        s = f"{x:.6f}"   # round to 6 decimals max
        s = s.rstrip('0').rstrip('.')  # remove unnecessary zeros
        return s
    if isinstance(x, int):
        return str(x)
    return str(x)


def load_data(fname):
    bin_data, meta = cs.read_cs_files(fname)
    df = pd.DataFrame(columns = meta[2], data=None)

    if bin_data != []:
        df['TIMESTAMP'] = pd.to_datetime(bin_data[0])
        for i, col in enumerate(meta[2][1:]):
            df[col] = bin_data[i + 1]
    return df, meta


def main():
    var = 'ts_data'
    dst_dir = f'/Users/pvn/Library/CloudStorage/OneDrive-OakRidgeNationalLaboratory/Shared/Projects/SETx-FluxData/{var}'
    src_dir = '/Users/pvn/Downloads/Download-2025-08-25'

    full_filenames = natsorted(glob.glob(os.path.join(src_dir, f'{var}*.dat')))    
    df_all = pd.DataFrame()
    for filename in full_filenames[29:]:
        print(filename)
        df, meta = load_data(filename)
        if meta[3][7] == 'umol/mol':
            df["CO2"] = df["CO2"]/44
            meta[3][7] = 'mmol/m^3'
        elif meta[3][7] == 'umol/m^3':
            meta[3][7] = 'mmol/m^3'

        if meta[3][8] == 'mmol/mol':
            df["H2O"] = df["H2O"]/0.018
            meta[3][8] = 'mmol/m^3'

        df_all = pd.concat([df_all, df], ignore_index=True)

    df_all['date'] = df_all["TIMESTAMP"].dt.date

    for day, df_day in df_all.groupby('date'):
        # Check if the day includes the last timestamp of the day
        last_ts = df_day["TIMESTAMP"].max()
        first_ts = df_day["TIMESTAMP"].min()        
        if last_ts.time() >= pd.to_datetime("23:59:59.900").time():
            hour = first_ts.hour
            minute = first_ts.minute

            # Format as 2-digit strings
            hour_str = f"{hour:02d}"
            minute_str = f"{minute:02d}"

            day_str = day.strftime("%Y-%m-%d")
            file_output = os.path.join(dst_dir, f'{var}_{day_str}_{hour_str}{minute_str}.dat')

            with open(file_output, "w", encoding="utf-8") as f:
                quoted_row = ['"{}"'.format(item) for item in meta[3]]
                f.write(','.join(quoted_row) + '\n')

            df_day.drop(columns=['date'], inplace=True)  # remove helper column
            formatted = df_day.copy()
            for col in df_day.columns:
                formatted[col] = df_day[col].apply(lambda x: format_value(x, is_timestamp=(col=="TIMESTAMP")))

            formatted.to_csv(
                os.path.join(dst_dir, file_output),
                index=False,
                quoting=3,
                mode="a",
            )
            print(f"Saved: {file_output}")
        else:
            print(f"Skipped {day}, incomplete day (last timestamp: {last_ts})")

    # write meta file for details
    metafile = os.path.join(dst_dir, 'meta.txt')
    # print(meta)
    with open(metafile, "w") as f:
        for row in meta:
            quoted_row = ['"{}"'.format(item) for item in row]
            f.write(','.join(quoted_row) + '\n')


if __name__ == "__main__":
    main()
