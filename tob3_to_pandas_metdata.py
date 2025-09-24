import os, glob
import csv
import pandas as pd
import read_cs_files as cs
import subprocess
from natsort import natsorted
from datetime import datetime

def load_data(fname):
    bin_data, meta = cs.read_cs_files(fname)
    df = pd.DataFrame(columns = meta[2], data=None)

    if bin_data != []:
        df['TIMESTAMP'] = pd.to_datetime(bin_data[0])
        for i, col in enumerate(meta[2][1:]):
            df[col] = bin_data[i + 1]
    return df, meta


def main():
    var = 'MetData'
    dst_dir = f'../../decoded_data/{var}'
    src_dir = '../../DataLogger/CRD/'
    cutoff = pd.Timestamp("2025-08-26 08:15:00")
    os.makedirs(dst_dir, exist_ok=True)
    full_filenames = natsorted(glob.glob(os.path.join(src_dir, f'{var}*.dat')))

    # 1. Read all files and combine
    df_all = pd.DataFrame()
    for filename in full_filenames:
        print("Reading:", filename)
        df, meta = load_data(filename)
        df_all = pd.concat([df_all, df], ignore_index=True)

    # 2. Apply cutoff (remove bad data during installation process)
    df_all = df_all[df_all["TIMESTAMP"] >= cutoff]
    if df_all.empty:
        print("No data after cutoff. EXIT!!!")
        exit()

    # 3. Add a year-month column
    df_all["year_month"] = df_all["TIMESTAMP"].dt.to_period("M")

    # 4. Determine already processed months
    processed_months = {
        f.split("_")[-1].replace(".csv", "")
        for f in os.listdir(dst_dir) if f.endswith(".csv")
    }

    # 5. Group by month and save
    for ym, group in df_all.groupby("year_month"):
        ym_str = str(ym)  # e.g., "2025-08"
        outfile = os.path.join(dst_dir, f"{var}_{ym_str}.csv")

        if ym_str in processed_months:
            print(f"Skipping {ym_str} (already processed)")
            continue

        print(f"Writing {outfile}")
        # Write header from meta first
        with open(outfile, "w", encoding="utf-8") as f:
            quoted_row = ['"{}"'.format(item) for item in meta[3]]
            f.write(",".join(quoted_row) + "\n")

        # Append month data
        group.drop(columns="year_month").to_csv(outfile, mode="a", index=False)

    # 6. Write metadata file once
    metafile = os.path.join(dst_dir, "meta.txt")
    with open(metafile, "w") as f:
        for row in meta:
            quoted_row = ['"{}"'.format(item) for item in row]
            f.write(",".join(quoted_row) + "\n")


if __name__ == "__main__":
    main()
