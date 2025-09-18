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
    dst_dir = f'/Users/pvn/Library/CloudStorage/OneDrive-OakRidgeNationalLaboratory/Shared/Projects/SETx-FluxData/{var}'
    src_dir = '/Users/pvn/Downloads/Download-2025-08-25'
    cutoff = pd.Timestamp("2025-08-26 08:15:00")

    full_filenames = natsorted(glob.glob(os.path.join(src_dir, f'{var}*.dat')))
    print(full_filenames)
    
    df_all = pd.DataFrame()
    for filename in full_filenames[0:]:
        print(filename)
        df, meta = load_data(filename)
        df_all = pd.concat([df_all, df], ignore_index=True)

    df_all = df_all[df_all["TIMESTAMP"] >= cutoff]
    first_date = df_all["TIMESTAMP"].dt.date.min()
    last_date  = df_all["TIMESTAMP"].dt.date.max()

    file_output = os.path.join(dst_dir, f'{var}_{first_date}_{last_date}.csv')
    print(file_output)    
    with open(file_output, "w", encoding="utf-8") as f:
        quoted_row = ['"{}"'.format(item) for item in meta[3]]
        f.write(','.join(quoted_row) + '\n')


    # write to csv file (variable names as headers)
    df_all.to_csv(os.path.join(dst_dir, file_output), mode="a", index=False)

    # write meta file for details
    metafile = os.path.join(dst_dir, 'meta.txt')
    # print(meta)
    with open(metafile, "w") as f:
        for row in meta:
            quoted_row = ['"{}"'.format(item) for item in row]
            f.write(','.join(quoted_row) + '\n')


if __name__ == "__main__":
    main()
