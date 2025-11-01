

"""
ETL Merge Script for Screaming Frog, GSC, and GA4 Exports
---------------------------------------------------------
This script reads exported CSV files from Screaming Frog (SEO Spider), Google Search Console (GSC), and Google Analytics 4 (GA4),
merges them on the URL field, cleans and normalizes the data, and saves the merged result.
Optionally, the merged data can be uploaded to Google Sheets.

Instructions:
1. Export your data from Screaming Frog, GSC, and GA4 as CSV files.
2. Place the files in the 'data/' folder or specify their paths below.
3. Run this script: python etl_merge.py
"""

# ------------------------------ Imports ------------------------------
import pandas as pd
import os
import sys
from typing import Optional

# Uncomment the following lines if you want to enable Google Sheets upload
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials

# ------------------------ Configuration -----------------------------
# Specify your file paths here
SCREAMING_FROG_CSV = "data/screaming_frog_export.csv"
GSC_CSV = "data/gsc_export.csv"
GA4_CSV = "data/ga4_export.csv"
MERGED_OUTPUT = "merged/merged_visibility.csv"

# Set to True if you want to upload to Google Sheets
UPLOAD_TO_SHEETS = False
GOOGLE_SHEET_NAME = "Merged Visibility Data"

# ------------------------ Helper Functions --------------------------

def load_and_normalize_frog(filepath: str) -> pd.DataFrame:
    """
    Loads and normalizes Screaming Frog CSV export.
    Returns a DataFrame with standardized column names.
    """
    print(f"Loading Screaming Frog data from {filepath} ...")
    df = pd.read_csv(filepath)
    # Standardize or rename URL column to 'URL'
    if 'Address' in df.columns:
        df = df.rename(columns={'Address': 'URL'})
    # Keep only necessary columns (customize as needed)
    columns_to_keep = ['URL', 'Status Code', 'Title 1', 'Meta Description 1', 'Word Count']
    columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    df = df[columns_to_keep]
    print(f"Screaming Frog data: {df.shape[0]} rows loaded.")
    return df

def load_and_normalize_gsc(filepath: str) -> pd.DataFrame:
    """
    Loads and normalizes Google Search Console CSV export.
    Returns a DataFrame with standardized column names.
    """
    print(f"Loading GSC data from {filepath} ...")
    df = pd.read_csv(filepath)
    # Standardize or rename URL column to 'URL'
    if 'Page' in df.columns:
        df = df.rename(columns={'Page': 'URL'})
    # Keep only necessary columns (customize as needed)
    columns_to_keep = ['URL', 'Clicks', 'Impressions', 'CTR', 'Position']
    columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    df = df[columns_to_keep]
    print(f"GSC data: {df.shape[0]} rows loaded.")
    return df

def load_and_normalize_ga4(filepath: str) -> pd.DataFrame:
    """
    Loads and normalizes Google Analytics 4 CSV export.
    Returns a DataFrame with standardized column names.
    """
    print(f"Loading GA4 data from {filepath} ...")
    df = pd.read_csv(filepath)
    # Standardize or rename URL column to 'URL'
    # GA4 exports often have 'Page path + query string' or similar
    url_col = None
    for candidate in ['Page', 'Page path', 'Page path + query string', 'Landing page']:
        if candidate in df.columns:
            url_col = candidate
            break
    if url_col:
        df = df.rename(columns={url_col: 'URL'})
    else:
        print("Could not find a URL column in GA4 export. Please check your CSV.")
        sys.exit(1)
    # Keep only necessary columns (customize as needed)
    columns_to_keep = ['URL', 'Users', 'Sessions', 'Engaged sessions', 'Average engagement time']
    columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    df = df[columns_to_keep]
    print(f"GA4 data: {df.shape[0]} rows loaded.")
    return df

def clean_url(url: str) -> str:
    """
    Cleans and normalizes a URL for merging.
    """
    if not isinstance(url, str):
        return url
    url = url.strip()
    # Remove trailing slashes for consistency
    if url.endswith('/'):
        url = url[:-1]
    return url

def upload_to_google_sheets(df: pd.DataFrame, sheet_name: str):
    """
    Uploads the DataFrame to a Google Sheet.
    """
    print("Uploading to Google Sheets (not implemented in this template).")
    # Uncomment and configure the following lines if you want to enable upload:
    # scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # creds = ServiceAccountCredentials.from_json_keyfile_name('path/to/credentials.json', scope)
    # client = gspread.authorize(creds)
    # sheet = client.open(sheet_name).sheet1
    # sheet.clear()
    # sheet.update([df.columns.values.tolist()] + df.values.tolist())
    pass

# -------------------------- Main ETL Logic ---------------------------
def main():
    # Load and normalize each dataset
    frog_df = load_and_normalize_frog(SCREAMING_FROG_CSV)
    gsc_df = load_and_normalize_gsc(GSC_CSV)
    ga4_df = load_and_normalize_ga4(GA4_CSV)

    # Clean URL columns for all datasets
    for df in [frog_df, gsc_df, ga4_df]:
        df['URL'] = df['URL'].apply(clean_url)

    # Merge datasets on 'URL' (outer join to keep all URLs)
    print("Merging datasets ...")
    merged_df = pd.merge(frog_df, gsc_df, on='URL', how='outer', suffixes=('_Frog', '_GSC'))
    merged_df = pd.merge(merged_df, ga4_df, on='URL', how='outer')
    print(f"Merged data: {merged_df.shape[0]} rows.")

    # Fill NaN values with blanks or zeros where appropriate
    for col in merged_df.columns:
        if merged_df[col].dtype == 'O':  # Object/string columns
            merged_df[col] = merged_df[col].fillna('')
        else:
            merged_df[col] = merged_df[col].fillna(0)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(MERGED_OUTPUT)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Export merged data to CSV
    merged_df.to_csv(MERGED_OUTPUT, index=False)
    print(f"Merged data exported to {MERGED_OUTPUT}")

    # Optionally upload to Google Sheets
    if UPLOAD_TO_SHEETS:
        upload_to_google_sheets(merged_df, GOOGLE_SHEET_NAME)
        print("Upload to Google Sheets completed.")

if __name__ == "__main__":
    main()