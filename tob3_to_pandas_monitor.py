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
    var = 'MonitorCSAT'
    dst_dir = f'/Users/pvn/Library/CloudStorage/OneDrive-OakRidgeNationalLaboratory/Shared/Projects/SETx-FluxData/{var}'
    src_dir = '/Users/pvn/Downloads/Download-2025-08-25'
    # cutoff = pd.Timestamp("2025-08-26 08:15:00")

    full_filenames = natsorted(glob.glob(os.path.join(src_dir, f'{var}*.dat')))
    
    df_all = pd.DataFrame()
    for filename in full_filenames[0:]:
        print(filename)
        df, meta = load_data(filename)
        df_all = pd.concat([df_all, df], ignore_index=True)

    first_date = df_all["TIMESTAMP"].dt.date.min()
    last_date = df_all["TIMESTAMP"].dt.date.max()

    # resample from 5min to 30min
    df_all['TIMESTAMP'] = pd.to_datetime(df_all['TIMESTAMP'])
    df_all = df_all.set_index('TIMESTAMP')    
    df_30min = df_all.resample("30T").mean()

    # write meta file for details
    metafile = os.path.join(dst_dir, 'meta.txt')
    # print(meta)
    with open(metafile, "w", encoding="utf-8") as f:
        for row in meta:
            quoted_row = ['"{}"'.format(item) for item in row]
            f.write(','.join(quoted_row) + '\n')

    # write data for csv file (variable names as headers)
    file_output = os.path.join(dst_dir, f'{var}_{first_date}_{last_date}.csv')
    df_30min.to_csv(os.path.join(dst_dir, file_output), index=True)


if __name__ == "__main__":
    main()
