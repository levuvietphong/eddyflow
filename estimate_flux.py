import os
import argparse
import configparser
import calendar
import numpy as np
from datetime import datetime, timezone
import subprocess


def main():
    parser = argparse.ArgumentParser(description="Find number of days in a month.")

    # Add arguments for month and year
    parser.add_argument('-m', '--month', type=int, required=True, help='Month number (1–12)')
    parser.add_argument('-y', '--year', type=int, required=True, help='Year (e.g., 2025)')
    parser.add_argument('-p', '--proj', type=str, required=True, help='Project folder')
    parser.add_argument('-i', '--input', type=str, required=True, help='input filename')
    parser.add_argument('-f', '--meta', type=str, required=True, help='meta filename')
    parser.add_argument('-r', '--raw', type=str, required=True, help='folder for raw files')
    parser.add_argument('-o', '--out_path', type=str, required=True, help='folder for outputs')
    args = parser.parse_args()

    if not (1 <= args.month <= 12):
        parser.error("Month must be between 1 and 12.")

    # Compute days
    proj_dir  = args.proj
    file_eddypro = f'{proj_dir}/{args.input}'
    file_meta = args.meta
    dir_raw = args.raw
    dir_outpath = args.out_path

    year = args.year
    month = args.month

    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year

    prev_num_days = calendar.monthrange(prev_year, prev_month)[1]
    num_days = calendar.monthrange(year, month)[1]

    # Format to always be 2 digits
    prev_month_str = f"{prev_month:02d}"
    prev_num_days_str = f"{prev_num_days:02d}"
    prev_year_str = f"{prev_year:04d}"

    month_str = f"{month:02d}"
    num_days_str = f"{num_days:02d}"
    year_str = f"{year:04d}"

    filename =  f'{proj_dir}/software/eddyflow/EddyPro/setx_{year_str}{month_str}{num_days_str}.eddypro'

    config = configparser.ConfigParser()
    config.read(file_eddypro)

    # Project
    current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    config['Project']['creation_date'] = current_time
    config['Project']['last_change_date'] = current_time
    config['Project']['pr_start_date'] = f'{prev_year_str}-{prev_month_str}-{prev_num_days_str}'
    config['Project']['pr_end_date'] = f'{year_str}-{month_str}-{num_days_str}'
    config['Project']['pr_start_time'] = '23:30'
    config['Project']['pr_end_time'] = '23:30'
    config['Project']['file_name'] = filename
    config['Project']['proj_file'] = f'{proj_dir}/{file_meta}'
    config['Project']['out_path'] = f'{proj_dir}/{dir_outpath}/{year_str}-{month_str}'

    os.makedirs(f'{proj_dir}/{dir_outpath}/{year_str}-{month_str}', exist_ok=True)

    # FluxCorrection_SpectralAnalysis_General
    config['FluxCorrection_SpectralAnalysis_General']['sa_start_date'] = f'{prev_year_str}-{prev_month_str}-{prev_num_days_str}'
    config['FluxCorrection_SpectralAnalysis_General']['sa_start_time'] = '23:30'
    config['FluxCorrection_SpectralAnalysis_General']['sa_end_date'] = f'{year_str}-{month_str}-{num_days_str}'
    config['FluxCorrection_SpectralAnalysis_General']['sa_end_time'] = '23:30'
    config['FluxCorrection_SpectralAnalysis_General']['ex_file'] = f'{proj_dir}/{dir_outpath}/{year_str}-{month_str}/eddypro_1_fluxnet_adv.csv'

    # RawProcess_General
    config['RawProcess_General']['data_path'] = f'{proj_dir}/{dir_raw}'

    # RawProcess_TiltCorrection_Settings
    config['RawProcess_TiltCorrection_Settings']['pf_start_date'] = f'{year_str}-{month_str}-{num_days_str}'
    config['RawProcess_TiltCorrection_Settings']['pf_start_time'] = '23:30'
    config['RawProcess_TiltCorrection_Settings']['pf_end_date'] = f'{prev_year_str}-{prev_month_str}-{prev_num_days_str}'
    config['RawProcess_TiltCorrection_Settings']['pf_end_time'] = '23:30'

    # RawProcess_TimelagOptimization_Settings
    config['RawProcess_TimelagOptimization_Settings']['to_start_date'] = f'{year_str}-{month_str}-{num_days_str}'
    config['RawProcess_TimelagOptimization_Settings']['to_start_time'] = '23:30'
    config['RawProcess_TimelagOptimization_Settings']['to_end_date'] = f'{prev_year_str}-{prev_month_str}-{prev_num_days_str}'
    config['RawProcess_TimelagOptimization_Settings']['to_end_time'] = '23:30'


    # Write to a file
    with open(filename, 'w') as configfile:
        configfile.write(';EDDYPRO_PROCESSING\n')
        config.write(configfile, space_around_delimiters=False)

    print(f'{filename} file has been written!')

    print('Running flux estimation...')
    cmd = f'eddypro_rp -s linux {filename} > output.txt'
    subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    main()
