# Eddy Covariance Flux Tower Data Processing

This repository provides tools to process **eddy covariance flux tower data**, with a focus on converting **TOB3 files** (Campbell Scientific binary logger format) into **ASCII (text) files** for further analysis.

## Features

- Read and decode TOB3 binary files
- Export data into ASCII/CSV format
- Easy-to-use scripts for batch processing

## Requirements

- Python 3.8+
- Recommended packages:
  - `numpy`
  - `pandas`
  - `struct` (standard library, for binary parsing)
