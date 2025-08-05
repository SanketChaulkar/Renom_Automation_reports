import streamlit as st
import os
import re
import logging
import zipfile
import datetime
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
import openpyxl
import xlrd
import jinja2
import xlsxwriter
import pyodbc
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')


EDF = [ "GK03", "GK04", "KL01", "KL02", "KL03", "KL04", "KL05", "KL06", "SP01", "SP02", "SP03", "SP04", "SP05", "SP06","SP07", "SP08", "SP09", "SP10" ]
ZR = [ "ZR01", "ZR03", "ZR06", "ZR07", "ZR08", "ZR09", "ZR10" ]
Hero = [ "HC02", "HC03", "HC05", "HC06", "HC11", "HC14", "HC15", "HC16", "HC17", "HC18", "HC19", "HC20", "SBT20","SBT40", "SBT52", "SBT91", "DANT142", "DANT143", "DANT148", "DANT149", "DANT150", "DANT151", "DANT152","DANT153", "DANT241", "DANT289", "KBS05", "KBS06", "KBS07", "KBS10", "KBS34", "KBS47", "KBS65", "KBS66","KBS67", "KBS68", "KBS70", "KBS73", "KBS74", "KBS75", "KBS78", "KBS79", "KBS80", "KBS81", "KBS82", "KBS83" ]
LGE = [ "DANT 100", "DANT 101", "DANT 102", "DANT 105", "DANT 106", "DANT 107", "DANT 108", "DANT 109", "DANT 110","DANT 111", "DANT 112", "DANT 113", "DANT 114", "DANT 115", "DANT 116", "DANT 117", "DANT 16", "DANT 211","DANT 212", "DANT 229", "DANT 232", "DANT 233", "DANT 28", "DANT 299", "DANT 44", "DANT 45", "DANT 46","DANT 47", "DANT 48", "DANT 49", "DANT 57", "DANT 91", "DANT 94", "DANT 95", "DANT 96", "DANT 104", "DANT 118","DANT 120", "DANT 139", "DANT 140", "DANT 17", "DANT 18", "DANT 19", "DANT 20", "DANT 21", "DANT 22","DANT 222", "DANT 223", "DANT 225", "DANT 226", "DANT 227", "DANT 228", "DANT 23", "DANT 42", "DANT 52","DANT 53", "DANT 62", "DANT 63", "DANT 64", "DANT 65", "DANT 66", "DANT 67", "DANT 68", "DANT 90", "DANT 92","DANT 98", "DANT 99" ]
Oil_India = [ "DANT130", "DANT131", "DANT132", "DANT134", "DANT161", "DANT162", "DANT163", "DANT164", "DANT165","DANT167", "DANT168", "DANT169", "DANT170", "DANT242", "DANT245", "DANT246", "DANT247", "DANT248","DANT250", "DANT251", "DANT252", "DANT253", "DANT254", "DANT282", "DANT285", "DANT79", "DANT80" ]
Tata = [ "DANT124", "DANT126", "DANT128", "DANT129", "DANT135", "DANT136", "DANT145", "DANT243", "DANT244", "DANT281","DANT383", "DANT385", "DANT386", "KBS26", "KBS27", "KBS33", "KBS51", "KBS52", "KBS53" ]
Atria = [ "SVRT 100", "SVRT 101", "SVRT 102", "SVRT 116", "SVRT 124", "SVRT 129", "SVRT 134", "SVRT 22", "SVRT 23","SVRT 31", "SVRT 49", "SVRT 56", "SVRT 68", "SVRT 84", "SVRT 87", "SVRT 93" ]
Torrent = [ "DANT123", "DANT224", "DANT230", "DANT231", "DANT30", "DANT31", "DANT32", "DANT33", "DANT36", "DANT54","DANT81", "DANT83", "GGM02", "GGM03", "GGM04", "GGM09", "GGM10", "GGM109", "GGM110", "GGM117", "GGM126","GGM133", "GGM141", "GGM16", "GGM19", "NPYP 57", "NPYP 85", "NPYP 87", "NPYP3 113", "NPYP3 13", "NPYP3 14","NPYP3 142", "NPYP3 15", "NPYP3 155", "NPYP3 156", "NPYP3 158", "NPYP3 173", "NPYP3 174", "NPYP3 175","NPYP3 176", "NPYP3 28", "NPYP3 43", "NPYP3 44", "RJ4T 43", "RJ8T 001", "RJ8T 002", "RJ8T 88", "RJ8T98", "RJ9T 002", "RJ9T 003", "RJ9T 004", "RJ9T 007","RJ9T 101", "RJ9T 105", "RJ9T 21", "RJ9T 22", "RJ9T 36", "RJ9T 38", "RJ9T 41", "RJ9T 43", "RJ9T 46","RJ9T 58", "RJ9T 66", "RJ9T 73", "RJ9T 86", "RJ9T 88", "RJ9T 90", "RJPT 006", "RJPT 124", "RJPT 154","RJPT 155", "RJPT 162", "RJPT 165", "RJPT 166", "RJPT 168", "RJPT 170", "RJPT 175", "RJPT160" ]
BG_WIND = ["KHD08", "KHD114", "KHD13", "KHD14", "KHD19", "KHD20", "KHD31", "KHD32", "KHD33", "KHD34",'BHT01','BHT02','BHT05','BHT08','BHT13','BHT18','BHT19']

Kalorana = [ "GK03", "GK04", "KL01", "KL02", "KL03", "KL04", "KL05", "KL06", "SP01", "SP02", "SP03", "SP04", "SP05","SP06", "SP07", "SP08", "SP09", "SP10" ]
Khanapur = [ "HC02", "HC03", "HC05", "HC06", "HC11", "HC14", "HC15", "HC16", "HC17", "HC18", "HC19", "HC20", "SBT20","SBT40", "SBT52", "SBT91" ]
Mahidad = [ "GGM02", "GGM03", "GGM04", "GGM09", "GGM10", "GGM109", "GGM110", "GGM117", "GGM126", "GGM133", "GGM141","GGM16", "GGM19" ]
Tadipatri = [ "ZR01", "ZR03", "ZR06", "ZR07", "ZR08", "ZR09", "ZR10" ]
Dangri = [ "DANT142", "DANT143", "DANT148", "DANT149", "DANT150", "DANT151", "DANT152", "DANT153", "DANT241", "DANT289","KBS05", "KBS06", "KBS07", "KBS10", "KBS34", "KBS47", "KBS65", "KBS66", "KBS67", "KBS68", "KBS70", "KBS73","KBS74", "KBS75", "KBS78", "KBS79", "KBS80", "KBS81", "KBS82", "KBS83", "DANT 104", "DANT 118", "DANT 120","DANT 139", "DANT 140", "DANT 17", "DANT 18", "DANT 19", "DANT 20", "DANT 21", "DANT 22", "DANT 222","DANT 223", "DANT 225", "DANT 226", "DANT 227", "DANT 228", "DANT 23", "DANT 42", "DANT 52", "DANT 53","DANT 62", "DANT 63", "DANT 64", "DANT 65", "DANT 66", "DANT 67", "DANT 68", "DANT 90", "DANT 92", "DANT 98","DANT 99", "DANT 100", "DANT 101", "DANT 102", "DANT 105", "DANT 106", "DANT 107", "DANT 108", "DANT 109","DANT 110", "DANT 111", "DANT 112", "DANT 113", "DANT 114", "DANT 115", "DANT 116", "DANT 117", "DANT 16","DANT 211", "DANT 212", "DANT 229", "DANT 232", "DANT 233", "DANT 28", "DANT 299", "DANT 44", "DANT 45","DANT 46", "DANT 47", "DANT 48", "DANT 49", "DANT 57", "DANT 91", "DANT 94", "DANT 95", "DANT 96", "DANT130","DANT131", "DANT132", "DANT134", "DANT161", "DANT162", "DANT163", "DANT164", "DANT165", "DANT167", "DANT168","DANT169", "DANT170", "DANT242", "DANT245", "DANT246", "DANT247", "DANT248", "DANT250", "DANT251", "DANT252","DANT253", "DANT254", "DANT282", "DANT285", "DANT79", "DANT80", "DANT124", "DANT126", "DANT128", "DANT129","DANT135", "DANT136", "DANT145", "DANT243", "DANT244", "DANT281", "DANT383", "DANT385", "DANT386", "KBS26","KBS27", "KBS33", "KBS51", "KBS52", "KBS53", "DANT123", "DANT224", "DANT230", "DANT231", "DANT30", "DANT31","DANT32", "DANT33", "DANT36", "DANT54", "DANT81", "DANT83","KHD08", "KHD114", "KHD13", "KHD14", "KHD19", "KHD20", "KHD31", "KHD32", "KHD33", "KHD34" ]
Nipaniya = [ "NPYP 57", "NPYP 85", "NPYP 87", "NPYP3 113", "NPYP3 13", "NPYP3 14", "NPYP3 142", "NPYP3 15", "NPYP3 155","NPYP3 156", "NPYP3 158", "NPYP3 173", "NPYP3 174", "NPYP3 175", "NPYP3 176", "NPYP3 28", "NPYP3 43","NPYP3 44" ]
Savarkundla = [ "SVRT 100", "SVRT 101", "SVRT 102", "SVRT 116", "SVRT 124", "SVRT 129", "SVRT 134", "SVRT 22","SVRT 23", "SVRT 31", "SVRT 49", "SVRT 56", "SVRT 68", "SVRT 84", "SVRT 87", "SVRT 93" ]
# Rojmal = ["RJ4T 43", "RJ8T 001", "RJ8T 002", "RJ8T 88", "RJ8T98", "RJ9T 002", "RJ9T 003", "RJ9T 004", "RJ9T 007","RJ9T 101", "RJ9T 105", "RJ9T 21", "RJ9T 22", "RJ9T 36", "RJ9T 38", "RJ9T 41", "RJ9T 43", "RJ9T 46","RJ9T 58", "RJ9T 66", "RJ9T 73", "RJ9T 86", "RJ9T 88", "RJ9T 90", "RJPT 006", "RJPT 124", "RJPT 154","RJPT 155", "RJPT 162", "RJPT 165", "RJPT 166", "RJPT 168", "RJPT 170", "RJPT 175", "RJPT160"]
Rojmal = [ "RJ4T 43", "RJ8T 001", "RJ8T 002", "RJ8T 88", "RJ8T98", "RJ9T 002", "RJ9T 003", "RJ9T 004", "RJ9T 007","RJ9T 101", "RJ9T 105", "RJ9T 21", "RJ9T 22", "RJ9T 36", "RJ9T 38", "RJ9T 41", "RJ9T 43", "RJ9T 46","RJ9T 58", "RJ9T 66", "RJ9T 73", "RJ9T 86", "RJ9T 88", "RJ9T 90", "RJPT 006", "RJPT 124", "RJPT 154","RJPT 155", "RJPT 162", "RJPT 165", "RJPT 166", "RJPT 168", "RJPT 170", "RJPT 175", "RJPT160" ]
Bhendewade = ['BHT01','BHT02','BHT05','BHT08','BHT13','BHT18','BHT19']


max_length = max(len(Kalorana), len(Khanapur), len(Mahidad), len(Tadipatri), len(Dangri), len(Nipaniya), len(Savarkundla), len(Rojmal),len(Bhendewade))
Kalorana += [ '' ] * (max_length - len(Kalorana))
Khanapur += [ '' ] * (max_length - len(Khanapur))
Mahidad += [ '' ] * (max_length - len(Mahidad))
Tadipatri += [ '' ] * (max_length - len(Tadipatri))
Dangri += [ '' ] * (max_length - len(Dangri))
Nipaniya += [ '' ] * (max_length - len(Nipaniya))
Savarkundla += [ '' ] * (max_length - len(Savarkundla))
Rojmal += [ '' ] * (max_length - len(Rojmal))
Bhendewade += [ '' ] * (max_length - len(Bhendewade))

# Ensure all lists have the same length
max_length1 = max(len(EDF), len(ZR), len(Hero), len(LGE), len(Oil_India), len(Tata), len(Atria), len(Torrent),len(BG_WIND))
EDF += [ '' ] * (max_length1 - len(EDF))
ZR += [ '' ] * (max_length1 - len(ZR))
Hero += [ '' ] * (max_length1 - len(Hero))
LGE += [ '' ] * (max_length1 - len(LGE))
Oil_India += [ '' ] * (max_length1 - len(Oil_India))
Tata += [ '' ] * (max_length1 - len(Tata))
Atria += [ '' ] * (max_length1 - len(Atria))
Torrent += [ '' ] * (max_length1 - len(Torrent))
BG_WIND += [ '' ] * (max_length1 - len(BG_WIND))

# Create the DataFrame
customer_name = pd.DataFrame({'EDF': EDF,'ZR': ZR,'Hero': Hero,'LGE': LGE,'Oil India': Oil_India,'Tata': Tata,'Atria': Atria,'Torrent': Torrent,'BG_WIND': BG_WIND})

site_name = pd.DataFrame({'Kalorana': Kalorana,'Khanapur': Khanapur,'Mahidad': Mahidad,'Tadipatri': Tadipatri,'Dangri': Dangri,'Nipaniya': Nipaniya,'Savarkundla': Savarkundla,'Rojmal': Rojmal,'Bhendewade' : Bhendewade})

# Function to retrieve the stored date

# stored_folder_link = r'Z:\INOX\Inox Daily\Aug-24\18-08\Error'

# stored_date = '2024-8-18'

def inox_error():
    # Retrieve the stored date string and folder path
    
    date_str = stored_date
    print(f'date_str Date:', {date_str})
    folder_path = stored_folder_link

    # Convert the date string back to a datetime object
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Simulate some processing
    st.write(f"Inox Error Module Executed!")
    st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
    st.write(f"Folder Path: {folder_path}")

    print(f'parsed_date Date:', {parsed_date})

    # Check if folder_path and date are not None
    if folder_path is None:
        print("Error: No folder link provided.")
        return
    if date_str is None:
        print("Error: No date provided.")
        return

    # Initialize an empty list to store DataFrames
    dfs = []

    try:
        # Iterate through all files in the directory
        for filename in os.listdir(folder_path):
            if filename.endswith('.xls') or filename.endswith('.xlsx'):  # Filter Excel files
                # Read the Excel file into a DataFrame
                filepath = os.path.join(folder_path, filename)
                df = pd.read_excel(filepath)

                # Append the DataFrame to the list
                dfs.append(df)

        # Check if any DataFrames were read
        if dfs:
            # Combine all DataFrames into one (optional, if needed)
            exist_df = pd.concat(dfs, ignore_index=True)
            print(f"Combined DataFrame with {len(exist_df)} rows and {len(exist_df.columns)} columns.")
            # Further processing can be done here
        else:
            print("No Excel files found in the provided folder.")

    except Exception as e:
        print(f"Error processing files in folder {folder_path}: {e}")

    # Concatenate all DataFrames into a single DataFrame

    final_df = pd.concat(dfs, ignore_index=True)

    df2 = final_df.dropna(subset=[ final_df.columns[ 0 ] ])
    df3 = df2[ df2.iloc[ :, 0 ] != "Error time" ]

    # Rename columns with increasing column numbers
    new_columns = [ 'Column_' + str(i) for i in range(len(df3.columns)) ]
    df3.columns = new_columns

    exist_df = df3[ [ 'Column_0', 'Column_2', 'Column_5', 'Column_8', 'Column_10', 'Column_11', 'Column_12' ] ]

    exist_df = exist_df.rename(columns={'Column_0': 'Error_time'})
    exist_df = exist_df.rename(columns={'Column_2': 'Turbine'})
    exist_df = exist_df.rename(columns={'Column_5': 'Duration'})
    exist_df = exist_df.rename(columns={'Column_8': 'Error_Code'})
    exist_df = exist_df.rename(columns={'Column_10': 'Error_description'})
    exist_df = exist_df.rename(columns={'Column_11': 'Site'})
    exist_df = exist_df.rename(columns={'Column_12': 'Customer'})

    # First, ensure that the 'Error time' column is in datetime format
    exist_df['Error_time'] = pd.to_datetime(exist_df['Error_time'])

    exist_df = exist_df[exist_df['Error_time'].dt.date == pd.to_datetime(date_str).date()]

    for col in exist_df.columns:
        if col != 'Error_Description':
            exist_df = exist_df.loc[ exist_df[ col ] != '-' ]

    # Iterate through the rows of exist_df
    for index, row in exist_df.iterrows():
        turbine = row[ 'Turbine' ]
        # Iterate through the columns of site_name DataFrame
        for col in site_name.columns:
            # Check if the turbine exists in the current column of site_name
            if turbine in site_name[ col ].values:
                exist_df.at[ index, 'Site' ] = str(col)  # Add the index name of the column to 'Site' column
                break  # Break the loop if the turbine is found

    # Iterate through the rows of exist_df
    for index, row in exist_df.iterrows():
        turbine = row[ 'Turbine' ]
        # Iterate through the columns of site_name DataFrame
        for col in customer_name.columns:
            # Check if the turbine exists in the current column of site_name
            if turbine in customer_name[ col ].values:
                exist_df.at[ index, 'Customer' ] = str(col)  # Add the index name of the column to 'Site' column
                break  # Break the loop if the turbine is found

    # Define the critical error codes as a list
    critical_error_codes = [ 4013, 4136, 7009, 7011, 7042, 7043, 7044, 7050, 7051, 7052, 7082, 7083, 7084, 7106, 7139, 7170, 7171, 7172, 7180, 7238, 7541, 9005, 9010, 9015, 9017, 9020, 9035, 9042, 9047, 9049, 9054, 9059, 9062, 9063, 9067, 9068, 9075, 9083, 9085, 10007, 10015, 10016, 10023, 10033, 10041, 11012, 11021, 12128, 21004, 21004, 21012, 21012, 22012, 28008, 28026, 31001, 31005, 31034, 21008, 21061, 26001, 26002, 26009, 26010, 26015, 26016, 26017,26014,26012,26011,26013,9086]

    # Create a DataFrame for critical error codes
    df_critical = pd.DataFrame({'critical_error_code': critical_error_codes})

    # Function to check if error code is critical or moderate
    def check_error_type(error_number):
        if error_number in df_critical['critical_error_code'].values:
            return 'Critical'
        else:
            return 'Moderate'

    # Apply the function to create a new column in exist_df
    exist_df['Type'] = exist_df['Error_Code'].apply(check_error_type)

    # Parse the selected date to extract year, month, and day
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Extract year, month, and day
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    # Now, let's insert these values into the pivot_df DataFrame
    exist_df['Year'] = year
    exist_df['Month'] = month
    exist_df['Day'] = day

    # Define a dictionary to map integer representations of months to month names
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

    # Replace integer values in the 'Month' column with corresponding month names
    exist_df['Month'] = exist_df['Month'].map(month_names)

    exist_df = exist_df[ ['Day','Month','Year','Error_time','Turbine','Site','Customer','Type','Duration','Error_Code','Error_description' ] ]


    # SQL connection parameters
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''  # Provide username if needed
    password = ''  # Provide password if needed
    table_name = 'Inox_Errors'

    # Create a connection string
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Define a function to check if a record exists based on all columns
    def record_exists(day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description):
        query = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE Day = ? AND Month = ? AND Year = ? AND ISNULL(Error_time, '') = ISNULL(?, '') AND ISNULL(Turbine, '') = ISNULL(?, '')
        AND ISNULL(Site, '') = ISNULL(?, '') AND ISNULL(Customer, '') = ISNULL(?, '') AND ISNULL(Type, '') = ISNULL(?, '')
        AND ISNULL(Duration, '') = ISNULL(?, '') AND Error_Code = ? AND ISNULL(Error_description, '') = ISNULL(?, '')
        """
        cursor.execute(query, (day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description))
        return cursor.fetchone()[0] > 0

    # Define a function to insert records
    def insert_record(day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description):
        query = f"""
        INSERT INTO {table_name} (Day, Month, Year, Error_time, Turbine, Site, Customer, Type, Duration, Error_Code, Error_description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            cursor.execute(query, (day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description))
        except pyodbc.Error as e:
            print(f"Error during insert: {e}")

    # Convert 'Duration' column to timedelta and then format to 'HH:MM:SS'
    def format_duration(duration):
        if pd.isna(duration):
            return None
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours:02}:{minutes:02}:{seconds:02}"

    # Simulate setting stored_date and folder_path
    def initialize_variables():
        global stored_date, stored_folder_link
        date_str = stored_date
        folder_path = stored_folder_link

        if folder_path is None:
            st.error("Error: No folder link provided.")
            return None, None
        if date_str is None:
            st.error("Error: No date provided.")
            return None, None

        # Convert the date string back to a datetime object
        try:
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            st.error("Error: Date format is incorrect.")
            return None, None

        # Simulate some processing
        st.write(f"Inox Error Module Executed!")
        st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
        st.write(f"Folder Path: {folder_path}")

        return parsed_date, folder_path

    # Initialize variables
    parsed_date, folder_path = initialize_variables()

    if parsed_date is not None and folder_path is not None:
        try:

            # Convert 'Duration' column to timedelta and then format to 'HH:MM:SS'
            exist_df['Duration'] = pd.to_timedelta(exist_df['Duration'], errors='coerce')
            exist_df['Duration'] = exist_df['Duration'].apply(format_duration)

            # Convert 'Error_time' to datetime, handling None values
            exist_df['Error_time'] = pd.to_datetime(exist_df['Error_time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            exist_df['Error_time'] = exist_df['Error_time'].fillna(pd.NaT)

            # Replace None and NaN with SQL Server's NULL representation
            exist_df.replace({pd.NA: None, np.nan: None}, inplace=True)

            # Start a transaction
            conn.autocommit = False

            try:
                # Process each record
                for index, row in exist_df.iterrows():
                    try:
                        print(f"Processing record {index}: {row.to_dict()}")  # Print out the row data for debugging

                        day = row['Day']
                        month = row['Month']
                        year = row['Year']
                        error_time = row['Error_time']
                        turbine = row['Turbine']
                        site = row['Site']
                        customer = row['Customer']
                        error_type = row['Type']
                        duration = row['Duration']
                        error_code = row['Error_Code']
                        error_description = row['Error_description']

                        # Check if the exact record exists in the SQL table
                        if not record_exists(day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description):
                            # If the record doesn't exist, insert it
                            insert_record(day, month, year, error_time, turbine, site, customer, error_type, duration, error_code, error_description)

                    except Exception as e:
                        print(f"Error processing record {index}: {e}")

                # Commit the transaction
                conn.commit()
            except Exception as e:
                print(f"Transaction error: {e}")
                conn.rollback()
        finally:
            # Close the connection
            cursor.close()
            conn.close()



    # SQL connection parameters
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''  # Provide username if needed
    password = ''  # Provide password if needed
    table_name = 'Inox_Errors'

    # Create a connection string
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # Example date_str, assume this is provided
    # date_str = '2024-09-02'

    # Convert the date string back to a datetime object
    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Date format is incorrect.")
        parsed_date = None

    if parsed_date:
        # Calculate the start date 30 days before the parsed_date
        start_date = parsed_date - timedelta(days=30)

        # Define the end date to include the full day for parsed_date
        end_date = parsed_date + timedelta(days=1)  # To include the entire day of the parsed_date

        # Establish the connection
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # SQL query to select records from the last 30 days including the specific date
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE Error_time >= ? AND Error_time < ?
        """

        # Execute the query using parameterized inputs
        df_last_30_days = pd.read_sql(query, conn, params=[start_date, end_date])

        # Close the connection
        cursor.close()
        conn.close()

        # Display the new DataFrame
        print(df_last_30_days)
    else:
        print("Error: Date is not valid.")


    # Convert 'Fault Duration (hh:mm:ss)' to timedelta for accurate summation
    exist_df['Duration'] = pd.to_timedelta(exist_df['Duration'])
    
    # Apply pivot table
    pivot_table = pd.pivot_table(exist_df,
                                index=['Site', 'Customer', 'Turbine', 'Type', 'Error_Code', 'Error_description'],
                                values=['Error_time', 'Duration'],
                                aggfunc={'Error_time':'count','Duration': 'sum'})

    # Convert pivot table to DataFrame and reset index
    pivot_df = pd.DataFrame(pivot_table.to_records())

    # Sort the DataFrame by the 'Count' column in descending order
    pivot_df = pivot_df.sort_values(by='Error_time', ascending=False)


    # Convert 'Fault Duration (hh:mm:ss)' to hh:mm:ss format
    pivot_df['Duration'] = pivot_df['Duration'].apply(lambda x: str(x)[-8:])

    pivot_df = pivot_df.rename(columns={'Error_time': 'Error_Count'})
    pivot_df = pivot_df.rename(columns={'Duration': 'Duration (hh:mm:ss)'})
    
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision', 3,
                        'expand_frame_repr', False):
        print(pivot_table)
    #
    pivot_df['Remarks'] = ''




    # # Convert 'Fault Duration (hh:mm:ss)' to timedelta for accurate summation
    # df_nun['Fault Duration (hh:mm:ss)'] = pd.to_timedelta(df_nun['Fault Duration (hh:mm:ss)'])

    # # Create the pivot table
    # pivot_table = pd.pivot_table(
    #     df_nun,
    #     index=['State', 'Site', 'Turbine Model', 'Turbine Name', 'Error Severity', 'Error_Code', 'Error Type', 'Error_Description'],
    #     values=['Start_DateTime', 'Fault Duration (hh:mm:ss)'],
    #     aggfunc={'Start_DateTime': 'count', 'Fault Duration (hh:mm:ss)': 'sum'}
    # )

    # # Reset index to make the pivot table easier to work with
    # pivot_table = pivot_table.reset_index()

    # pivot_table = pivot_table.rename(columns={'Start_DateTime': 'Error_Count'})

    # # Convert 'Fault Duration (hh:mm:ss)' to hh:mm:ss format
    # pivot_table['Fault Duration (hh:mm:ss)'] = pivot_table['Fault Duration (hh:mm:ss)'].apply(lambda x: str(x)[-8:])

    # # Sort the DataFrame by the 'Count' column in descending order
    # pivot_table = pivot_table.sort_values(by='Error_Count', ascending=False)






    # # Apply pivot table
    # pivot_df_new = new_df.pivot_table(
    #     index=['Turbine', 'Site', 'Customer', 'Type', 'Error_Code', 'Error_description'],  # Rows
    #     columns=['Day', 'Month'],  # Columns
    #     values='Year',  # Values to count
    #     aggfunc='count',  # Aggregation function: count occurrences
    #     # fill_value=0  # Fill missing values with 0
    # )

    # # Convert pivot table to DataFrame and reset index
    # pivot_df_new = pd.DataFrame(pivot_df_new.to_records())

    # # List to store new column names
    # new_columns = []

    # # Iterate through each column
    # for col in df.columns:
    #     if col.startswith("(") and "," in col:
    #         # Parse the string to extract day and month
    #         day = col.split(",")[0].strip("(").strip()
    #         month = col.split(",")[1].strip(")").strip(" '")
    #         new_columns.append(f'{month}_Day{day}')  # Rename to 'Month_DayX'
    #     else:
    #         # Keep the original name for non-day/month columns
    #         new_columns.append(col)

    # # Apply the new column names to the DataFrame
    # df.columns = new_columns



    # Apply pivot table
    pivot_df_new = df_last_30_days.pivot_table(
        index=['Turbine', 'Site', 'Customer', 'Type', 'Error_Code', 'Error_description'],  # Rows
        columns=['Day', 'Month'],  # Columns
        values='Year',  # Values to count
        aggfunc='count',  # Aggregation function: count occurrences
        # fill_value=0  # Fill missing values with 0
    )

    # Rename the pivot table columns by combining Day and Month
    pivot_df_new.columns = [f'{day} {month}' for day, month in pivot_df_new.columns]

    # Convert pivot table to DataFrame and reset index
    # pivot_df_new = pd.DataFrame(pivot_df_new.to_records())


    # Function to extract sortable components from column name
    def extract_date_info(col_name):
        try:
            day, month = col_name.split(' ', 1)
            return (month, int(day))
        except ValueError:
            return (col_name, 0)

    # Sort columns based on extracted date info
    sorted_columns = sorted(pivot_df_new.columns, key=extract_date_info)

    # Reorder columns in the DataFrame
    pivot_df_new = pivot_df_new[sorted_columns]

    # Convert pivot table to DataFrame and reset index
    pivot_df_new = pd.DataFrame(pivot_df_new.to_records())

    # Calculate the sum of date values and add it as "Grand Total"
    date_columns = [col for col in pivot_df_new.columns if ' ' in col]  # Identify date columns
    pivot_df_new['Grand Total'] = pivot_df_new[date_columns].sum(axis=1)

    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    # Format the date for the filename as dd-mm-yyyy
    formatted_filename_date = parsed_date.strftime('%d-%m-%Y')

    # Create the file name in the required format
    file_name = f"{formatted_filename_date} Inox Daily Error Report.xlsx"

    # Define the file path using the function name
    # file_name = f"{'Inox Daily Error Report'}.xlsx"

    # Combine the folder path and file name
    save_path = os.path.join(folder_path, file_name)

    # Save the DataFrames to the specified Excel file
    with pd.ExcelWriter(save_path) as writer:
        exist_df.to_excel(writer, sheet_name='Raw Data For Upload', index=False)
        pivot_df.to_excel(writer, sheet_name='Error Remarks Sheet', index=False)
        df_last_30_days.to_excel(writer, sheet_name='Repeated Error Data', index=False)
        pivot_df_new.to_excel(writer, sheet_name='Repeated Errors', index=False)


    # with pd.ExcelWriter('D:/python/Inox Daily Error Report 21-07-2024.xlsx') as writer:
    #     exist_df.to_excel(writer, sheet_name='Raw Data For Upload', index=False)
    #     pivot_df.to_excel(writer, sheet_name='Error Remarks Data', index=False)

    return

def inox_warning(): 
       
        
    # Retrieve the stored date string and folder path
    date_str = stored_date
    print(f'date_str Date:', {date_str})
    folder_path = stored_folder_link

    # Convert the date string back to a datetime object
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Simulate some processing
    st.write(f"Inox Error Module Executed!")
    st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
    st.write(f"Folder Path: {folder_path}")

    print(f'parsed_date Date:', {parsed_date})

    # Check if folder_path and date are not None
    if folder_path is None:
        print("Error: No folder link provided.")
        return
    if date_str is None:
        print("Error: No date provided.")
        return

    # Initialize an empty list to store DataFrames
    dfs = [ ]

    # Iterate through all files in the directory
    for filename in os.listdir(folder_link):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):  # Filter Excel files
            # Read the Excel file into a DataFrame
            filepath = os.path.join(folder_link, filename)
            df = pd.read_excel(filepath)

            # Append the DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)

    # First, ensure that the 'Error time' column is in datetime format
    final_df['Time on'] = pd.to_datetime(final_df['Time on'])

    # Select data for the selected date
    final_df = final_df[final_df['Time on'].dt.date == pd.to_datetime(date_str).date()]

    final_df = final_df.rename(columns={'Unnamed: 0': 'Alarm'})

    # Iterate through the rows of exist_df
    for index, row in final_df.iterrows():
        Device = row[ 'Device' ]
        # Iterate through the columns of site_name DataFrame
        for col in site_name.columns:
            # Check if the turbine exists in the current column of site_name
            if Device in site_name[ col ].values:
                final_df.at[ index, 'Site' ] = str(col)  # Add the index name of the column to 'Site' column
                break  # Break the loop if the turbine is found

    # Iterate through the rows of exist_df
    for index, row in final_df.iterrows():
        Device = row[ 'Device' ]
        # Iterate through the columns of site_name DataFrame
        for col in customer_name.columns:
            # Check if the turbine exists in the current column of site_name
            if Device in customer_name[ col ].values:
                final_df.at[ index, 'Customer' ] = str(col)  # Add the index name of the column to 'Site' column
                break  # Break the loop if the turbine is found


    final_df = final_df[ [ 'Site', 'Customer', 'Alarm', 'Device', 'Description', 'Time on', 'Time off', 'Alarm status'  ] ]


    # Select rows where the value in the 'Alarm' column is 'Warning'
    filtered_df = final_df[final_df['Alarm'] == 'Warning']

    # Parse the selected date to extract year, month, and day
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Extract year, month, and day
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    # Now, let's insert these values into the pivot_df DataFrame
    filtered_df['Year'] = year
    filtered_df['Month'] = month
    filtered_df['Day'] = day

    # Define a dictionary to map integer representations of months to month names
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

    # Replace integer values in the 'Month' column with corresponding month names
    filtered_df['Month'] = filtered_df['Month'].map(month_names)

    filtered_df = filtered_df[ ['Day','Month','Year','Site', 'Customer', 'Alarm', 'Device', 'Description', 'Time on', 'Time off', 'Alarm status' ] ]


    # Select rows where the value in the 'Alarm' column is 'Warning'
    Error_df = final_df[final_df['Alarm'] == 'Error']

    Error_df = Error_df[ [ 'Site', 'Customer', 'Alarm', 'Device', 'Description', 'Time on', 'Time off', 'Alarm status'  ] ]

    # Parse the selected date to extract year, month, and day
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Extract year, month, and day
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    # Now, let's insert these values into the pivot_df DataFrame
    Error_df['Year'] = year
    Error_df['Month'] = month
    Error_df['Day'] = day

    # Define a dictionary to map integer representations of months to month names
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

    # Replace integer values in the 'Month' column with corresponding month names
    Error_df['Month'] = Error_df['Month'].map(month_names)


    # Function to split the error code and description
    def split_description(text):
        # Use regex to extract the error code and description
        match = re.search(r'\((\d+)\)\s*(.*)', text)
        if match:
            return match.group(1), match.group(2)
        return None, None

    # Apply the function to split all the 'description' column values
    Error_df[['Error_Code', 'Error_Description']] = Error_df['Description'].apply(lambda x: pd.Series(split_description(x)))
    
    Error_df['Time on'] = pd.to_datetime(Error_df['Time on'], errors='coerce')
    Error_df['Time off'] = pd.to_datetime(Error_df['Time off'], errors='coerce')

    # Calculate duration
    Error_df['Duration'] = Error_df['Time off'] - Error_df['Time on']

    # Convert to HH:MM:SS string format
    Error_df['Duration'] = Error_df['Duration'].apply(lambda x: str(x).split('.')[0])  # removes microseconds


    Error_df = Error_df[ ['Day','Month','Year','Site', 'Customer', 'Alarm', 'Device', 'Error_Code','Error_Description', 'Time on', 'Time off', 'Alarm status','Duration' ] ]


    # Apply pivot table
    # pivot_df_error = Error_df.groupby(['Site', 'Customer', 'Alarm', 'Device', 'Error_Code','Error_Description', 'Alarm status']).size().reset_index(name='Count')

    # Ensure Duration is timedelta
    Error_df['Duration'] = pd.to_timedelta(Error_df['Duration'])

    # Group and aggregate
    pivot_df_error = Error_df.groupby(['Site', 'Customer', 'Alarm', 'Device','Error_Code', 'Error_Description', 'Alarm status']).agg( Count=('Time on', 'count'), Total_Duration=('Duration', 'sum')
    ).reset_index()


    # Format timedelta to HH:MM:SS properly
    pivot_df_error['Total_Duration'] = pivot_df_error['Total_Duration'].apply(
        lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
    )


    # Define the critical error codes as a list
    critical_error_codes = ["Blade 1: Safety run is active.","Blade 2: Safety run is active.","Blade 3: Safety run is active.","Brake chain - Emergency button not reset.","Brake chain - fuse opened.","Grid voltage is not OK.","Pitch is not started because of wrong Blade State.","Pitch is started but the Capacitor State is not hysteresis Charge.","Safety chain - Emergency button pressed.","Safety chain - Fuse opened.","Safety chain - Rotor brake closed.","Phase monitoring relay triggered.","PLC PM3000 state machine is in error state (Initialization failed, or other error)..","Grid frequency from Sineax above operation range.","Grid frequency from Sineax below operation range.","Grid frequency from converter below operation range.","Blade 1: Too big deviations from model (inside hysteresis).","Blade 2: Too big deviations from model (inside hysteresis).","Blade 3: Too big deviations from model (inside hysteresis).","Blade 1: Can Communication failure (Slave or Master not in operational state or Guarding error).","Blade 2: Can Communication failure (Slave or Master not in operational state or Guarding error).","Blade 3: Can Communication failure (Slave or Master not in operational state or Guarding error).","Blade 1: Blade Grid loss (Udc is near Battery voltage).","Blade 1: Blade Grid loss (Udc is near Battery voltage).","Blade 3: Blade Grid loss (Udc is near Battery voltage).","Converter LSA: Positive 15V power supply feedback out of range.","Converter LSA: Negative 15V power supply feedback out of range.","Converter LSA: 5ms CAN message Fault.","Converter LSA: 5ms triggered CAN message Fault.","Converter GSA: Power Supply Fault 5V.","Converter GSA: Positive 15V power supply feedback out of range.","Converter GSA: Negative 15V power supply feedback out of range.","Converter GSA: 5ms CAN message Fault.","Converter GSA: 5ms triggered CAN message Fault.","Gearbox oil filter 75% clogged.","Safety chain - High vibration detected by vibration switch.","SLC: Tower vibration supervision module in error state.","All tower-vibration sensors in drivetrain direction defective","All tower-vibration sensors in non-drivetrain direction defective","Offset of the tower-vibration sensor in drivetrain direction above error limit","Offset of the tower-vibration sensor in non-drivetrain direction above error limit","Tower vibration (acceleration peak-to-peak value) in drivetrain direction delayed is above limit","Tower vibration (acceleration peak-to-peak value) in non-drivetrain direction delayed is above limit","More than 3 tower-vibration (acceleration peak-peak) warnings within specified time"]

    # Function to check if a description matches any critical warning
    def check_critical_error(Error_Code):
        for warning in critical_error_codes:
            if warning in Error_Code:
                return "Critical"
        return "Moderate"

    # Apply the function to create a new column indicating criticality
    pivot_df_error['Severity'] = pivot_df_error['Error_Description'].apply(check_critical_error)

    pivot_df_error['Remarks'] = ''

    pivot_df_error = pivot_df_error[ [ 'Site', 'Customer', 'Alarm', 'Device','Severity', 'Error_Code','Error_Description','Count', 'Total_Duration', 'Alarm status','Remarks' ] ]


    # Sort the DataFrame by the 'Count' column in descending order
    pivot_df_error = pivot_df_error.sort_values(by='Count', ascending=False)
 

    filtered_df['Time on'] = pd.to_datetime(filtered_df['Time on'], errors='coerce')
    filtered_df['Time off'] = pd.to_datetime(filtered_df['Time off'], errors='coerce')

    # Calculate duration
    filtered_df['Duration'] = filtered_df['Time off'] - filtered_df['Time on']

    # Convert to HH:MM:SS string format
    # filtered_df['Duration'] = filtered_df['Duration'].apply(lambda x: str(x).split('.')[0])  # removes microseconds

    # Apply pivot table
    # pivot_df = filtered_df.groupby(['Site', 'Customer', 'Alarm', 'Device', 'Description', 'Alarm status']).size().reset_index(name='Count')

   # Group and aggregate
    pivot_df = filtered_df.groupby(['Site', 'Customer', 'Alarm', 'Device', 'Description', 'Alarm status']).agg( Count=('Time on', 'count'), Total_Duration=('Duration', 'sum')
    ).reset_index()

    # Format timedelta to HH:MM:SS properly
    pivot_df['Total_Duration'] = pivot_df['Total_Duration'].apply(
        lambda x: f"{int(x.total_seconds() // 3600):02}:{int((x.total_seconds() % 3600) // 60):02}:{int(x.total_seconds() % 60):02}"
    )


    # Sort the DataFrame by the 'Count' column in descending order
    pivot_df = pivot_df.sort_values(by='Count', ascending=False)

    # Function to split the error code and description
    def split_description(text):
        # Use regex to extract the error code and description
        match = re.search(r'\((\d+)\)\s*(.*)', text)
        if match:
            return match.group(1), match.group(2)
        return None, None

    # Apply the function to split all the 'description' column values
    pivot_df[['Warning_Code', 'Warning_Description']] = pivot_df['Description'].apply(lambda x: pd.Series(split_description(x)))

    # List of critical warnings
    critical_warnings = ["Line choke temperature above warning limit.", "Water cooling plate temperature above warning limit.", "CC100 generator choke temperature sensor is defective (Short cut or wire break).", "Converter cabinet high temperature power reduction.", "GSC IGBT temperature above warning limit.", "Converter system power reduction is active.", "Gearbox main bearing temperature 1 below error limit.", "Gearbox main bearing temperature 2 below error limit.", "Gearbox main bearing temperature 2 above warning limit.", "Gearbox main bearing temperature 3 below error limit.", "Gearbox main bearing temperature 3 above warning limit.", "Gearbox rotor bearing temperature below error limit.", "Gearbox oil filter 75% clogged.", "Gearbox oil filter system pressure below error limit.", "Gearbox oil input pressure below error limit.", "Gearbox oil input temperature below error limit.", "Gearbox oil tank temperature below error limit.", "Gearbox oil tank temperature above warning limit.", "Gearbox high temperature power reduction.", "Gearbox low temperature power and/or speed reduction.", "Power reduction caused by gearbox temperature.", "Generator rotor surge arrestor feedback is not OK.", "Generator stator surge arrestor feedback is not OK.", "Generator bearing temperature DE above warning limit.", "Generator bearing temperature NDE above warning limit.", "Generator grounding brush worn error.", "Grid voltage based power reduction is active.", "Temperature difference between pitch motors too high.", "Generator cooling water input temperature above warning limit.", "Analog environment temperature sensors defective.", "Generator brush warning.", ]

    # Function to check if a description matches any critical warning
    def check_critical(description):
        for warning in critical_warnings:
            if warning in description:
                return "Critical"
        return "Moderate"

    # Apply the function to create a new column indicating criticality

    pivot_df['Severity'] = pivot_df['Warning_Description'].apply(check_critical)


    # Parse the selected date to extract year, month, and day
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Extract year, month, and day
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    # Now, let's insert these values into the pivot_df DataFrame
    pivot_df['Year'] = year
    pivot_df['Month'] = month
    pivot_df['Day'] = day

    # Define a dictionary to map integer representations of months to month names
    month_names = {1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June', 7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'}

    # Replace integer values in the 'Month' column with corresponding month names
    pivot_df['Month'] = pivot_df['Month'].map(month_names)

    # Create a new DataFrame where the status is 'On'

    live_warning_df = pivot_df[pivot_df['Alarm status'] == 'On']

    live_warning_df['Remarks'] = ''

    live_warning_df = live_warning_df[ [ 'Site', 'Customer', 'Alarm', 'Device', 'Warning_Code', 'Warning_Description', 'Count', 'Total_Duration', 'Alarm status','Remarks' ] ]

    # Sort the DataFrame by the 'Count' column in descending order

    pivot_df = pivot_df.sort_values(by='Count', ascending=False)



    pivot_df = pivot_df[ ['Day', 'Month', 'Year', 'Site', 'Customer', 'Alarm', 'Device','Severity', 'Warning_Code', 'Warning_Description', 'Count', 'Total_Duration', 'Alarm status'] ]

    # Create a new DataFrame where the status is 'On'
    critical_warning_df = pivot_df[pivot_df['Severity'] == 'Critical']

    critical_warning_df['Remarks'] = ''

    critical_warning_df = critical_warning_df[ [ 'Site', 'Customer', 'Alarm', 'Device', 'Warning_Code', 'Warning_Description', 'Count','Total_Duration','Severity', 'Alarm status','Remarks' ] ]

    # Create a new DataFrame where the description contains 'reduction'

    reduction_df = pivot_df[pivot_df['Warning_Description'].str.contains('reduction', case=False)]

    reduction_df['Remarks'] = ''

    reduction_df = reduction_df[ [ 'Site', 'Customer', 'Alarm', 'Device', 'Warning_Code', 'Warning_Description', 'Count', 'Total_Duration', 'Alarm status','Remarks' ] ]

    pivot_df_new = pivot_df[ ['Day', 'Month', 'Year', 'Site', 'Customer', 'Device','Severity', 'Warning_Code', 'Warning_Description', 'Count'] ]

    # SQL connection parameters
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''  # Provide username if needed
    password = ''  # Provide password if needed
    table_name = 'Inox_Warning'

    # Create a connection string
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Define a function to check if a record exists based on all columns
    def record_exists(day, month, year, site, customer, device, severity, warning_code, warning_description, count):
        query = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE Day = ? AND Month = ? AND Year = ? AND ISNULL(Site, '') = ISNULL(?, '')
        AND ISNULL(Customer, '') = ISNULL(?, '')  AND ISNULL(Device, '') = ISNULL(?, '') AND ISNULL(Severity, '') = ISNULL(?, '')
        AND Warning_Code = ? AND ISNULL(Warning_Description, '') = ISNULL(?, '')
        AND ISNULL(Count, '') = ISNULL(?, '')
        """
        cursor.execute(query, (day, month, year, site, customer, device, severity, warning_code, warning_description, count))
        return cursor.fetchone()[0] > 0

    # Define a function to insert records
    def insert_record(day, month, year, site, customer, device, severity, warning_code, warning_description, count):
        query = f"""
        INSERT INTO {table_name} (Day, Month, Year, Site, Customer, Device, Severity, Warning_Code, Warning_Description, Count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        try:
            cursor.execute(query, (day, month, year, site, customer, device, severity, warning_code, warning_description, count))
        except pyodbc.Error as e:
            print(f"Error during insert: {e}")

    # Simulate setting stored_date and folder_path
    def initialize_variables():
        global stored_date, stored_folder_link
        date_str = stored_date
        folder_path = stored_folder_link

        if folder_path is None:
            st.error("Error: No folder link provided.")
            return None, None
        if date_str is None:
            st.error("Error: No date provided.")
            return None, None

        # Convert the date string back to a datetime object
        try:
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            st.error("Error: Date format is incorrect.")
            return None, None

        # Simulate some processing
        st.write(f"Inox Error Module Executed!")
        st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
        st.write(f"Folder Path: {folder_path}")

        return parsed_date, folder_path

    # Initialize variables
    parsed_date, folder_path = initialize_variables()

    if parsed_date is not None and folder_path is not None:
        try:

            # Replace None and NaN with SQL Server's NULL representation
            pivot_df_new.replace({pd.NA: None, np.nan: None}, inplace=True)

            # Start a transaction
            conn.autocommit = False

            try:
                # Process each record
                for index, row in pivot_df_new.iterrows():
                    try:
                        print(f"Processing record {index}: {row.to_dict()}")  # Print out the row data for debugging

                        day = row['Day']
                        month = row['Month']
                        year = row['Year']
                        site = row['Site']
                        customer = row['Customer']
                        device = row['Device']
                        severity = row['Severity']
                        warning_code = row['Warning_Code']
                        warning_description = row['Warning_Description']
                        count = row['Count']

                        # Check if the exact record exists in the SQL table
                        if not record_exists(day, month, year, site, customer, device, severity, warning_code, warning_description, count):
                            # If the record doesn't exist, insert it
                            insert_record(day, month, year, site, customer, device, severity, warning_code, warning_description, count)

                    except Exception as e:
                        print(f"Error processing record {index}: {e}")

                # Commit the transaction
                conn.commit()
            except Exception as e:
                print(f"Transaction error: {e}")
                conn.rollback()
        finally:
            # Close the connection
            cursor.close()
            conn.close()


    # SQL connection parameters
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''  # Provide username if needed
    password = ''  # Provide password if needed
    table_name = 'Inox_Warning'

    # Create a connection string
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # Example date_str, assume this is provided
    # date_str = '2024-10-16'

    # Convert the date string back to a datetime object
    try:
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Date format is incorrect.")
        parsed_date = None

    if parsed_date:
        # Calculate the start date 30 days before the parsed_date
        start_date = parsed_date - timedelta(days=30)
        
        # Define the end date to include the full day for parsed_date
        end_date = parsed_date + timedelta(days=1)  # To include the entire day of the parsed_date

        # Establish the connection
        conn = pyodbc.connect(conn_str)

        # Updated SQL query that converts Year, Month, and Day into a proper date
        query = f"""
        SELECT Day, Month, Year, Site, Customer, Device, Severity, Warning_Code, Warning_Description, [Count]
        FROM {table_name}
        WHERE CAST(CONCAT(Year, '-', 
                CASE 
                    WHEN Month = 'January' THEN '01'
                    WHEN Month = 'February' THEN '02'
                    WHEN Month = 'March' THEN '03'
                    WHEN Month = 'April' THEN '04'
                    WHEN Month = 'May' THEN '05'
                    WHEN Month = 'June' THEN '06'
                    WHEN Month = 'July' THEN '07'
                    WHEN Month = 'August' THEN '08'
                    WHEN Month = 'September' THEN '09'
                    WHEN Month = 'October' THEN '10'
                    WHEN Month = 'November' THEN '11'
                    WHEN Month = 'December' THEN '12'
                END, '-', Day) AS DATE) 
        BETWEEN ? AND ?
        """

        # Execute the query using parameterized inputs and load data into a DataFrame
        df_last_30_days = pd.read_sql(query, conn, params=[start_date, end_date])

        # Close the connection
        conn.close()

        # Print the DataFrame to check the result
        print(df_last_30_days)


    # Apply pivot table
    pivot_df_new = df_last_30_days.pivot_table(
        index=['Device', 'Site', 'Customer', 'Severity', 'Warning_Code', 'Warning_Description'],  # Rows
        columns=['Day', 'Month'],  # Columns
        values='Count',  # Assuming you are counting occurrences
        aggfunc='sum',  # Aggregation function: sum occurrences
        # fill_value=0  # Fill missing values with 0
    )

    # Rename the pivot table columns by combining Day and Month
    pivot_df_new.columns = [f'{day} {month}' for day, month in pivot_df_new.columns]

    # Convert pivot table to DataFrame and reset index
    pivot_df_new = pd.DataFrame(pivot_df_new.to_records())

    # Function to extract sortable components from column name (day and month)
    def extract_date_info(col_name):
        try:
            day, month = col_name.split(' ', 1)
            return (month, int(day))  # Sort by month, then by day
        except ValueError:
            return (col_name, 0)

    # Sort columns based on extracted date info
    sorted_columns = sorted([col for col in pivot_df_new.columns if ' ' in col], key=extract_date_info)

    # Reorder columns: Keep index columns first, followed by sorted date columns, then 'Grand Total'
    index_columns = ['Device', 'Site', 'Customer', 'Severity', 'Warning_Code', 'Warning_Description']
    pivot_df_new = pivot_df_new[index_columns + sorted_columns]

    # Calculate the sum of date values and add it as "Grand Total"
    pivot_df_new['Grand Total'] = pivot_df_new[sorted_columns].sum(axis=1)

    # Convert pivot table to DataFrame and reset index
    pivot_df_new.reset_index(drop=True, inplace=True)


    Monthly_reduction_df = df_last_30_days[df_last_30_days['Warning_Description'].str.contains('reduction', case=False)]
    
    # Apply pivot table
    pivot_df_Power_Reduction = Monthly_reduction_df.pivot_table(
        index=['Device', 'Site', 'Customer', 'Severity', 'Warning_Code', 'Warning_Description'],  # Rows
        columns=['Day', 'Month'],  # Columns
        values='Count',  # Assuming you are counting occurrences
        aggfunc='sum',  # Aggregation function: sum occurrences
        # fill_value=0  # Fill missing values with 0
    )

    # Rename the pivot table columns by combining Day and Month
    pivot_df_Power_Reduction.columns = [f'{day} {month}' for day, month in pivot_df_Power_Reduction.columns]

    # Convert pivot table to DataFrame and reset index
    pivot_df_Power_Reduction = pd.DataFrame(pivot_df_Power_Reduction.to_records())

    # Function to extract sortable components from column name (day and month)
    def extract_date_info(col_name):
        try:
            day, month = col_name.split(' ', 1)
            return (month, int(day))  # Sort by month, then by day
        except ValueError:
            return (col_name, 0)

    # Sort columns based on extracted date info
    sorted_columns = sorted([col for col in pivot_df_Power_Reduction.columns if ' ' in col], key=extract_date_info)

    # Reorder columns: Keep index columns first, followed by sorted date columns, then 'Grand Total'
    index_columns = ['Device', 'Site', 'Customer', 'Severity', 'Warning_Code', 'Warning_Description']
    pivot_df_Power_Reduction = pivot_df_Power_Reduction[index_columns + sorted_columns]

    # Calculate the sum of date values and add it as "Grand Total"
    pivot_df_Power_Reduction['Grand Total'] = pivot_df_Power_Reduction[sorted_columns].sum(axis=1)

    # Convert pivot table to DataFrame and reset index
    pivot_df_Power_Reduction.reset_index(drop=True, inplace=True)


    pivot_df['Remarks'] = ''

    pivot_df = pivot_df[ ['Day', 'Month', 'Year', 'Site', 'Customer', 'Alarm', 'Device','Severity', 'Warning_Code', 'Warning_Description', 'Count','Total_Duration', 'Alarm status','Remarks' ] ]



    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    # Format the date for the filename as dd-mm-yyyy
    formatted_filename_date = parsed_date.strftime('%d-%m-%Y')

    # Create the file name in the required format
    file_name = f"{formatted_filename_date} Inox Daily Warning Report.xlsx"

    # Define the file path using the function name
    # file_name = f"{'Inox Daily Warning Report'}.xlsx"

    # Combine the folder path and file name
    save_path = os.path.join(folder_path, file_name)

    # Save the DataFrames to the specified Excel file
    with pd.ExcelWriter(save_path) as writer:
        filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
        pivot_df.to_excel(writer, sheet_name='Warning Data', index=False)
        live_warning_df.to_excel(writer, sheet_name='Live Warning', index=False)
        critical_warning_df.to_excel(writer, sheet_name='Critical Warning', index=False)
        reduction_df.to_excel(writer, sheet_name='Power Reduction Warning', index=False)
        Error_df.to_excel(writer, sheet_name='Error Raw Data', index=False)
        pivot_df_error.to_excel(writer, sheet_name='Message Error Sheet', index=False)
        df_last_30_days.to_excel(writer, sheet_name='Repeated Data', index=False)
        pivot_df_new.to_excel(writer, sheet_name='Repeated Warnings', index=False)
        Monthly_reduction_df.to_excel(writer, sheet_name='Power Reduction Data', index=False)
        pivot_df_Power_Reduction.to_excel(writer, sheet_name='Repted Power Reduc warning', index=False)



    # # Save the updated DataFrame to the Excel file
    # with pd.ExcelWriter('D:/python/Inox Daily Warning report 01-08-2024.xlsx') as writer:
    #     filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
    #     pivot_df.to_excel(writer, sheet_name='Warning Data', index=False)
    #     live_warning_df.to_excel(writer, sheet_name='Live Warning', index=False)
    #     critical_warning_df.to_excel(writer, sheet_name='Critical Warning', index=False)
    #     reduction_df.to_excel(writer, sheet_name='Power Reduction Warning', index=False)
    #     Error_df.to_excel(writer, sheet_name='Error Raw Data', index=False)
    #     pivot_df_error.to_excel(writer, sheet_name='Message Error Data', index=False)

    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision', 3,
    #                        'expand_frame_repr', False):
    #     print(final_df)
    return

def resca_error() :

    date_str = stored_date
    print(f'date_str Date:', {date_str})
    folder_path = stored_folder_link

    # Convert the date string back to a datetime object
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Simulate some processing
    st.write(f"Inox Error Module Executed!")
    st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
    st.write(f"Folder Path: {folder_path}")

    print(f'parsed_date Date:', {parsed_date})

    # Check if folder_path and date are not None
    if folder_path is None:
        print("Error: No folder link provided.")
        return
    if date_str is None:
        print("Error: No date provided.")
        return

    # folder_path = r"Z:\RESCA\Resca Daily\August-24\26-08"
    # date_str='2024-08-26'
    # # Read the CSV file into a DataFrame, specifying the encoding and the delimiter
    # df = pd.read_excel(file_path)
    df=[]

    try:
        # Iterate through all files in the directory
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv') or filename.endswith('.xlsx'):  # Filter Excel files
                # Read the Excel file into a DataFrame
                filepath = os.path.join(folder_path, filename)
                df = pd.read_excel(filepath)

    except Exception as e:
        print(f"Error processing files in folder {folder_path}: {e}")

    # Assuming 'df' is your DataFrame containing the data
    df['Turbine Model'] = df['Turbine Model'].replace('V82-II', 'V82')

    df1 = df[['Year of Start_DateTime','Month of Start_DateTime','Day of Start_DateTime','State', 'Site', 'Turbine Model', 'Turbine Name', 'Error Code ','Error Type', 'Error Description', 'Start_DateTime', 'End_DateTime','Fault Duration (hh:mm:ss)', 'Fault Duration (min)', 'Wind speed' ]]

    df1 = df1.rename(columns={'Month of Start_DateTime': 'Month'})
    df1 = df1.rename(columns={'Year of Start_DateTime': 'Year'})
    df1 = df1.rename(columns={'Day of Start_DateTime': 'Day'})
		

    error_code=['Test security system','W Lack of wind Rotor speed too low','Cable twisted Left (1 turn)','Cable twisted Right (1 turn)','Turbine reset Power failure','Turbine reset Quit button','Turbine reset Scada system','Grid error Undervoltage L1','Grid error Overvoltage L1+L2+L3','Grid error Undervoltage L2','Grid error Undervoltage L3','Grid error Overvoltage L1','Grid error start delay','Grid error Overvoltage L3','Grid error Underfrequency','Grid error Overfrequency','Turbine stopped Scada system (ENERCON)','Turbine moist Inverter','Test security system','W Lack of wind Rotor speed too low','Cable twisted Left (2-3 turns)','Turbine reset Power failure','Generator heating Mains breakdown','Turbine reset Scada system','W Lack of wind Rotor speed too low','Generator heating Hygrostat inverter','Cable twisted Left (2-3 turns)','Cable twisted Right (2-3 turns)','Turbine reset Power failure','Turbine reset Scada system','Grid error Undervoltage L1','Grid error Undervoltage L2','Grid error Undervoltage L3','Grid error Overvoltage L1','Grid error Overvoltage L2','Grid error Overvoltage L3','Grid error Underfrequency','Grid error start delay','Grid error Undervoltage L1+L2+L3','Turbine stopped Scada system (ENERCON)','Storm Average wind speed','Turbine moist Inverter','Turbine moist Inverter 2','W Lack of wind Rotor speed too low','Cable twisted Left (2-3 turns)','Cable twisted Right (2-3 turns)','Cable twisted Left (>3 turns)','Grid error UndervoltageL1','Grid error UndervoltageL2','Grid error UndervoltageL3','Grid error Overvoltage L3','Turbine stopped Scada system (ENERCON)','Turbine moist Inverter','Test security system','W Lack of wind Rotor speed too low','Cable twisted Left (2-3 turns)','Maintenance','Cable twisted Right (2-3 turns)','Turbine reset Power failure','Turbine reset Scada system','Grid error UndervoltageL1','Grid error UndervoltageL2','Grid error UndervoltageL3','Grid error Overvoltage L2','Grid Spikes','Grid Drop Voltage','Service','Extr. low voltage L _ : _ V','ExEx low voltage L _ : _ V','Too many auto-restarts: _','Extr. high volt. L _ : _ V','Extr. low voltage L _ : _ V','High voltage L _ : _ V','Low voltage L _ : _ V','ExEx low voltage L _ : _ V','Stand Still(Status)','Maintenance(Status)','error_grid_frequency','error_converter_signal_phase_voltage_peak','Turbine stopped(status)','error_pitch_position_end_switch_3','Grid spikes L1','Grid spikes L2','Grid drop voltage L1','Grid drop voltage L2','Grid drop voltage L3','Service key','Stand Still(Status)','Maintenance(Status)','Turbine stopped(status)','-','Not Available','Remote stop','Low voltage L2','Low voltage L1','Low voltage L3','Grid spikes L3','Low frequency L1','Turbine stopped Control cabinet','Safety stop','High voltage L1','High voltage L2','Frequency error: _ Hz','Feeding control bus error (Bus-Off) Power Control','Feeding control bus error (Bus-Off) Power Control','Stop Via Data Line','Turbine moist Mains breakdown','Turbine moist Several inverters','Manual Stop Key Board','Turbine reset - Power failure','Turbine reset - Power failure','Turbine reset - Power failure','Cable twisted - Right (2-3 turns)','Cable twisted - Left (2-3 turns)','Cable twisted - Right (1 turn)','Turbine moist Inverter 1+2']

    # Create a DataFrame for critical error codes
    df_remove = pd.DataFrame({'error_code': error_code})

    # Function to check if error code is critical or moderate
    def check_error_type(error_number):
        if error_number in df_remove['error_code'].values:
            return 'Remove'
        else:
            return 'nun'

    # Apply the function to create a new column in exist_df
    df1['Type'] = df1['Error Description'].apply(check_error_type)

    # Filter df1 to create a new DataFrame where Type is 'nun'
    df_nun = df1[df1['Type'] == 'nun']

    critical_error=['Yaw control fault Change of nacelle position faulty','Yaw pads worn ',' Vibration sensor' ,'Tower oscillation Transversal oscillation','Tower oscillation Longitudinal oscillation','Tower oscillation Transversal oscillation (max.)','Tower oscillation Longitudinal oscillation (max.)','Speed sensor error Acceleration measurement','Monitoring switch  Battery box','Monitoring switch  Kingpin','Generator overtemperature Stator','Generator overtemperature Rotor','Slip clutch monitoring ','Bearing temperature Overtemp. front bearing','Bearing temperature Overtemp. rear bearing','Generator heating Isometer',' Vibration sensor ','Tower oscillation Transversal oscillation','Tower oscillation Longitudinal oscillation','Tower oscillation Transversal oscillation (max.)','Tower oscillation Longitudinal oscillation (max.)','Speed sensor error Acceleration measurement','Fault rectifier Earth contact system 1','Fault rectifier Earth contact system 2','Fault rectifier Earth contact system 1+2','Overtemperature Generator filter','Generator overtemperature Stator','Generator overtemperature Rotor','Torque monitoring Peak load','Bearing temperature Overtemp. front bearing','Bearing temperature Overtemp. rear bearing',' Generator heating Isometer',' Vibration sensor ','Tower oscillation Transversal oscillation','Tower oscillation Longitudinal oscillation','Tower oscillation Transversal oscillation (max.)','Tower oscillation Longitudinal oscillation (max.)','Speed sensor error Acceleration measurement','Fault lubrication system (7) Grease reservoir empty',' Feeding fault  Earth contact','Generator overtemperature Stator','Generator overtemperature Rotor','Air gap monitoring Sensor 1 blade A','Air gap monitoring Sensor 2 blade A','Air gap monitoring Sensor 1 blade B','Air gap monitoring Sensor 2 blade B','Air gap monitoring Sensor 1 blade C','Air gap monitoring Sensor 2 blade C','Air gap monitoring Several sensors','Air gap monitoring Both sensors blade A','Air gap monitoring Both sensors blade B','Air gap monitoring Both sensors blade C','Torque monitoring Peak load','Torque monitoring Overtemp. front bearing','Torque monitoring Overtemp. rear bearing','Generator heating Isometer','Vibration sensor ','Tower oscillation Transversal oscillation ','Tower oscillation Longitudinal oscillation ','Tower oscillation Transversal oscillation (max.) ','Tower oscillation Longitudinal oscillation (max.) ','Speed sensor error Acceleration measurement','Speed sensor error Acceleration measurement (-) ',' W Fault lubrication system (7) Grease reservoir empty ','Feeding fault Earth contact ','Generator overtemperature Stator ','Generator overtemperature Rotor','72 Air gap monitoring Sensor actuated (-) ','72 Air gap monitoring Sensor 1 blade A','72 Air gap monitoring Sensor 2 blade A ','72 Air gap monitoring Both sensors blade A ','72 Air gap monitoring Several sensors' ,'72 Air gap monitoring Sensor 1 blade B ','72 Air gap monitoring Sensor 2 blade B ','72 Air gap monitoring Both sensors blade B ','72 Air gap monitoring Several sensors ','72 Air gap monitoring Sensor 1 blade C ','72 Air gap monitoring Sensor 2 blade C ','72 Air gap monitoring Both sensors blade C ','72 Air gap monitoring Several sensors','73 Torque monitoring Peak load ','Bearing temperature Overtemp. front bearing','Bearing temperature Overtemp. rear bearing','Excitation error Fault current','Smoke detector Nacelle (main carrier) +','Smoke detector Nacelle (generator rear) +','Fault Transformer Overtemperatur (measurement) ','Automatic Test Valves Tips','Generator G feedback Missing','Hydraulic Error Wingtips Startup','Vibration Guard','Hydraulic Error Calipers','Brake Worn Svendborg','Generator Over Speed 1','TAC84 STOP Vibration','Vibrations','Generator G feedback Missing','Hydraulic Error Wingtips Restart','W Untwisting CCW','W Replace Battery','Control Voltage missing','Yaw Indicator Error','Rotor Over Speed','Thyristor Over Heat','Low Production Active','PT 100 Error','Emergency circuit open','Frequency error: _ Hz','Pitch too low: _ Â° < _ Â°','Power Curve _ m/s _ kW','PitchA pos.: _ Â° Vel: _ Â°/s','Hydr. temperature high: _ Â°C','Safety-pressostat brake','Emergency circuit open','High gear temperature: _ Â°C','Chock sensor trigged: _ RPM','Feedback = _ ,Brake','Max temperature thyristors','Low oil-level,hydraulic','Ambient temperature high: _ Â°C','High temp. bearing _ : _ Â°C','Error temp.sensor R _ ,_ Â°C','Thermoerror yawmotor F','No yawpulses _ ,_ s','High Oscillation Transm. _ m/s','Rotor: _ RPM,Gen.: _ RPM','Pitch too low: _ Â° < _ Â°','Rotor overspeed','Speed sensor fault TAC84','Pitch ac. pressure deviation','W Max missing lubr. generator','Generator overspeed 1','Generator overspeed 2','error_acceleration_nacelle_global','error_acceleration_nacelle_limit','error_acceleration_nacelle_limit','error_acceleration_nacelle_limit_offset','error_converter_main_contactor','error_converter_precharge_contactor','error_converter_generator_contactor','error_converter_generator_contactor','error_converter_monitoring_IGBT_global','error_converter_IGBT_ok','error_converter_step_up_IGBT','error_converter_step_up_IGBT','error_converter_chopper_IGBT','Overspeed guard TAC85','error_converter_chopper_IGBT','error_converter_grid_IGBT','error_converter_grid_IGBT','error_converter_signal_monitoring_global','error_converter_signal_DC_current_overcurrent','error_converter_signal_IGBT_overcurrent_peak','error_converter_signal_chopper_overcurrent','error_converter_igbt_temperature_global','error_converter_grid_igbt_temperature_global','error_converter_chopper_igbt_temperature','error_converter_grid_monitoring_U_DC_positive','error_converter_grid_monitoring_U_DC_negative','error_converter_grid_monitoring_chopper_I','error_converter_grid_monitoring_step_up_U_DC_limits','error_converter_grid_monitoring_udc_unsymmetry','error_converter_temperature_generator_capacitors','error_converter_temperature_dc_link_capacitor','error_converter_temperature_rectifier','error_generator_temperature_limit','error_generator_temperature_limit','error_wind_measurement_global','error_wind_large_change_of_winddirection','error_wind_anemometer','error_wind_wind_vane','error_safety_system_vibration_switch_nacelle','error_safety_system_cable_twist','Main CB 1 tripped','Asymmetric current fast','Cut-in overspeed','Thyristor fuse blown','Thyristor L1 open circuit','Thyristor L2 open circuit','Thyristor L3 open circuit','Thyristor L1 short circuit','Thyristor L2 short circuit','Thyristor L3 short circuit','Cut-in phase sequence fault','Cut-in avg. current L1','Cut-in avg. current L2','Cut-in avg. current L3','Phase sequence fault','Negative power gen. G','Negative power high','Generator G temp. high','Gen. G contactor open','By -pass contactor open','Gen. G contactor closed','By-pass contactor closed','Cut-in phase vector fault','Extreme flap moment protection','Extreme flap moment safety stop','Pitch oil low','Pitch oil temp. high','Gear oil temp. high long term','Vibration TAC84','error_safety_system_prog_ts_gr1_out_error','error_safety_system_prog_ts_gr1_com_error','Hydr. temperature high: _ ?øC','High temp. bearing _ : ?øC','Rotor overspeed','Speed sensor fault TAC84','Pitch ac. pressure deviation','W Max missing lubr. generator','Generator overspeed 1','Generator overspeed 2','Overspeed guard TAC85','Main CB 1 tripped','Asymmetric current fast','Cut-in overspeed','Thyristor fuse blown','Thyristor L1 open circuit','Thyristor L2 open circuit','Thyristor L3 open circuit','Thyristor L1 short circuit','Thyristor L2 short circuit','Thyristor L3 short circuit','Cut-in phase sequence fault','Cut-in avg. current L1','Cut-in avg. current L2','Cut-in avg. current L3','Phase sequence fault','Negative power gen. G','Negative power high','Generator G temp. high','Gen. G contactor open','By -pass contactor open','Gen. G contactor closed ','By-pass contactor closed','Cut-in phase vector fault','Extreme flap moment protection','Extreme flap moment safety stop','Pitch oil low','Pitch oil temp. high','Gear oil temp. high long term','Vibration TAC84',]

    df_nun = df_nun.rename(columns={'Error Code ': 'Error_Code'})

    # Your list of integers separated by colons
    data = ['22:52', '23:0', '30:0', '31:1', '31:2', '31:11', '31:12', '48:10', '50:10', '50:11', '70:1', '70:2', '74:0','76:1', '76:2', '9:1', '30:0', '31:1', '31:2', '31:11', '31:12', '48:10', '66:41', '66:42', '66:43', '67:3','70:1', '70:2', '73:2', '76:1', '76:2', '9:1', '30:0', '31:1', '31:2', '31:11', '31:12', '48:10', '58:1','62:43', '70:1', '70:2', '72:1', '72:2', '72:3', '72:4', '72:5', '72:6', '72:10', '72:11', '72:12', '72:13','73:2', '76:1', '76:2', '9:1', '30:0', '31:1', '31:2', '31:11', '31:12', '48:10', '48:12', '58:1', '62:*43','70:1', '70:2', '72:99', '72:101', '72:102', '72:110', '72:120', '72:201', '72:202', '72:210', '72:220','72:301', '72:302', '72:310', '72:320', '73:2', '76:01:00', '76:02:00', '80:01:00', '112:21:00', '112:22:00','122:101', '102', '35', '82', '91', '67', '34', '13', '100', '25', '30', '75', '38', '23', '89', '42', '12','18', '111', '114', '102', '129', '194', '323', '330', '167', '162', '102', '147', '156', '189', '176', '165','313', '150', '212', '186', '180', '370', '158', '194', '254', '391', '403', '333', '251', '252', '10000','10001', '10001', '10002', '30103', '30104', '30105', '30105', '30200', '30201', '30202', '30202', '30203','397', '30203', '30204', '30204', '30300', '30301', '30302', '30304', '30700', '30710', '30730', '30901','30902', '30904', '30905', '30906', '40001', '40002', '40003', '70001', '70001', '170000', '170001', '170010','170020', '800004', '800005', '504', '30', '253', '563', '820', '821', '822', '823', '824', '825', '981','987', '988', '989', '50', '87', '89', '106', '530', '532', '630', '632', '983', '237', '238', '359', '150','193', '393', '800052', '800053', '167', '150', '254', '391', '403', '333', '251', '252', '397', '504','30', '253', '563', '820', '821', '822', '823', '824', '825', '981', '987', '988', '989', '50', '87', '89','106', '530', '532', '630', '632', '983', '237', '238', '359', '150', '193', '393']

    # Creating DataFrame with the list and adding single inverted comma to each value
    df12 = pd.DataFrame(["'" + value + "'" for value in data], columns=['values'])

    # Function to check if an error code is critical or moderate
    def check_error_type(error_code):
        if error_code in critical_error:
            return 'Critical'
        else:
            return 'Moderate'

    # Apply the function to create a new column indicating the type of error
    df_nun['Error Severity'] = df_nun['Error Description'].apply(check_error_type)

    # Extract year, month, and day from date_str
    year, month, day = map(int, date_str.split('-'))

    # Create a mapping of month names to numbers
    month_mapping = {'January': 1, 'February': 2, 'March': 3, 'April': 4,'May': 5, 'June': 6, 'July': 7, 'August': 8,'September': 9, 'October': 10, 'November': 11, 'December': 12}

    # Filter the DataFrame without altering it
    df_nun = df_nun[(df_nun['Year'] == year) & (df_nun['Month'].map(month_mapping) == month) & (df_nun['Day'] == day)]

    # Convert 'Fault Duration (hh:mm:ss)' to timedelta for accurate summation
    df_nun['Fault Duration (hh:mm:ss)'] = pd.to_timedelta(df_nun['Fault Duration (hh:mm:ss)'])

    # Create the pivot table
    pivot_table = pd.pivot_table(
        df_nun,
        index=['State', 'Site', 'Turbine Model', 'Turbine Name', 'Error Severity', 'Error_Code', 'Error Type', 'Error Description'],
        values=['Start_DateTime', 'Fault Duration (hh:mm:ss)'],
        aggfunc={'Start_DateTime': 'count', 'Fault Duration (hh:mm:ss)': 'sum'}
    )

    # Reset index to make the pivot table easier to work with
    pivot_table = pivot_table.reset_index()

    pivot_table = pivot_table.rename(columns={'Start_DateTime': 'Error_Count'})

    # Convert 'Fault Duration (hh:mm:ss)' to hh:mm:ss format
    pivot_table['Fault Duration (hh:mm:ss)'] = pivot_table['Fault Duration (hh:mm:ss)'].apply(lambda x: str(x)[-8:])

    # Sort the DataFrame by the 'Count' column in descending order
    pivot_table = pivot_table.sort_values(by='Error_Count', ascending=False)

    pivot_table['Remarks'] = ''

    # Convert 'Fault Duration (hh:mm:ss)' to hh:mm:ss format
    df_nun['Fault Duration (hh:mm:ss)'] = df_nun['Fault Duration (hh:mm:ss)'].apply(lambda x: str(x)[-8:])

    df_nun = df_nun[ ['Year','Month','Day','State','Site','Turbine Model','Turbine Name','Error_Code','Error Severity','Error Type','Error Description','Start_DateTime','End_DateTime','Fault Duration (hh:mm:ss)','Wind speed']]




    # Define database connection details
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''
    password = ''
    table_name = 'dbo.Resca_Errors'

    # Create a connection string
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )

    # Establish the connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Define a function to check if a record exists based on all columns
    def record_exists(day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed):
        query = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE Day = ? AND Month = ? AND Year = ?
        AND State = ?
        AND ISNULL(Site, '') = ISNULL(?, '')
        AND ISNULL(Turbine_Model, '') = ISNULL(?, '')
        AND ISNULL(Turbine_Name, '') = ISNULL(?, '')
        AND ISNULL(Error_Code, '') = ISNULL(?, '')
        AND ISNULL(Error_Severity, '') = ISNULL(?, '')
        AND ISNULL(Error_Type, '') = ISNULL(?, '')
        AND ISNULL(Error_Description, '') = ISNULL(?, '')
        AND ISNULL(Start_DateTime, '') = ISNULL(?, '')
        AND ISNULL(End_DateTime, '') = ISNULL(?, '')
        AND ISNULL(Fault_Duration, '') = ISNULL(?, '')
        AND ISNULL(Wind_Speed, '') = ISNULL(?, '')
        """
        params = (day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed)
        try:
            cursor.execute(query, params)
            return cursor.fetchone()[0] > 0
        except pyodbc.Error as e:
            print(f"Error during record_exists check: {e}")
            print(f"SQL Query: {query}")
            print(f"Parameters: {params}")
            return False

    # Define a function to insert records
    def insert_record(day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed):
        query = f"""
        INSERT INTO {table_name}
        (Day, Month, Year, State, Site, Turbine_Model, Turbine_Name, Error_Code, Error_Severity, Error_Type, Error_Description, Start_DateTime, End_DateTime, Fault_Duration, Wind_Speed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed)
        try:
            cursor.execute(query, params)
        except pyodbc.Error as e:
            print(f"Error during insert: {e}")
            print(f"SQL Query: {query}")
            print(f"Parameters: {params}")

    # Replace None and NaN with SQL Server's NULL representation
    df_nun.replace({pd.NA: None, np.nan: None}, inplace=True)

    # Convert 'Start_DateTime', 'End_DateTime', and 'Fault Duration (hh:mm:ss)' to the appropriate types
    df_nun['Start_DateTime'] = pd.to_datetime(df_nun['Start_DateTime'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
    df_nun['End_DateTime'] = pd.to_datetime(df_nun['End_DateTime'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
    # df_nun['Fault Duration (hh:mm:ss)'] = pd.to_timedelta(df_nun['Fault Duration (hh:mm:ss)'], errors='coerce').dt.components.apply(lambda x: f"{x['hours']:02}:{x['minutes']:02}:{x['seconds']:02}", axis=1)

    # Start a transaction
    conn.autocommit = False

    try:
        # Process each record in df_nun
        for index, row in df_nun.iterrows():
            try:
                print(f"Processing record {index}: {row.to_dict()}")  # Print out the row data for debugging

                day = row['Day']
                month = row['Month']
                year = row['Year']
                state = row['State']
                site = row['Site']
                turbine_model = row['Turbine Model']
                turbine_name = row['Turbine Name']
                error_code = row['Error_Code']
                error_severity = row['Error Severity']
                error_type = row['Error Type']
                error_description = row['Error_Description']
                start_datetime = row['Start_DateTime']
                end_datetime = row['End_DateTime']
                fault_duration = row['Fault Duration (hh:mm:ss)']
                wind_speed = row['Wind speed']

                # Check if the exact record exists in the SQL table
                if not record_exists(day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed):
                    # If the record doesn't exist, insert it
                    insert_record(day, month, year, state, site, turbine_model, turbine_name, error_code, error_severity, error_type, error_description, start_datetime, end_datetime, fault_duration, wind_speed)
                    
            except Exception as e:
                print(f"Error processing record {index}: {e}")
        
        # Commit the transaction
        conn.commit()
    except Exception as e:
        print(f"Transaction error: {e}")
        conn.rollback()
    finally:
        # Close the connection
        cursor.close()
        conn.close()

    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    # Format the date for the filename as dd-mm-yyyy
    formatted_filename_date = parsed_date.strftime('%d-%m-%Y')

    # Create the file name in the required format
    file_name = f"{formatted_filename_date} Resca Daily Error Report.xlsx"

    # Define the file path using the function name
    # file_name = f"{'Resca Daily Error Report'}.xlsx"

    # Combine the folder path and file name
    save_path = os.path.join(folder_path, file_name)


    # Save the DataFrames to the specified Excel file
    with pd.ExcelWriter(save_path) as writer:
        # df.to_excel(writer, sheet_name='Original Data', index=False)
        # df1.to_excel(writer, sheet_name='Filtered data', index=False)
        df_nun.to_excel(writer, sheet_name='Final Upload Data', index=False)
        pivot_table.to_excel(writer, sheet_name='Error Remarks Sheet', index=False)

        
    # # Save the updated DataFrame to the Excel file
    # with pd.ExcelWriter('D:/python/resca daily.xlsx') as writer:
    #     # df.to_excel(writer, sheet_name='Original Data', index=False)
    #     # df1.to_excel(writer, sheet_name='Filtered data', index=False)
    #     df_nun.to_excel(writer, sheet_name='Final Upload Data', index=False)
    #     pivot_table.to_excel(writer, sheet_name='Error Remarks Sheet', index=False)

def gamesa_error():
    # Retrieve the stored date string and folder path
    date_str = stored_date

    folder_path = stored_folder_link

    # Convert the date string back to a datetime object
    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # Simulate some processing
    st.write(f"Gamesa Error Module Executed!")
    st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
    st.write(f"Folder Path: {folder_path}")


    # Check if folder_path and date are not None
    if folder_path is None:
        print("Error: No folder link provided.")
        return
    if date_str is None:
        print("Error: No date provided.")
        return

    # Initialize lists to store paths of extracted files
    extracted_files = []


    def read_file_with_encoding(file_path, delimiter=';'):
        try:
            if file_path.endswith('.xlsx'):
                return pd.read_excel(file_path)
            elif file_path.endswith('.csv'):
                return pd.read_csv(file_path, encoding='utf-8', delimiter=delimiter)
        except UnicodeDecodeError:
            try:
                return pd.read_csv(file_path, encoding='latin1', delimiter=delimiter)
            except Exception as e:
                print(f"Failed to read {file_path}: {e}")
                return pd.DataFrame()  # Return an empty DataFrame on failure
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on failure


    extracted_files = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.zip'):
            zip_path = os.path.join(folder_path, filename)
            base_name = filename[:-4]
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = [file_info.filename for file_info in zip_ref.infolist() if file_info.filename.endswith(('.xlsx', '.csv'))]

                for i, file_name in enumerate(file_list):
                    if i < 3:
                        new_filename = f"{base_name}_{file_name}"
                        extracted_path = os.path.join(folder_path, new_filename)
                        with zip_ref.open(file_name) as source_file, open(extracted_path, 'wb') as target_file:
                            target_file.write(source_file.read())
                        extracted_files.append(extracted_path)


    # Read the first file into df1
    if len(extracted_files) > 0:
        df1 = read_file_with_encoding(extracted_files[0])

    # Read the second file into df2
    if len(extracted_files) > 1:
        df2 = read_file_with_encoding(extracted_files[1])

    # Read the second file into df2
    if len(extracted_files) > 2:
        df3 = read_file_with_encoding(extracted_files[2])

        # Read the second file into df2
    if len(extracted_files) > 3:
        df4 = read_file_with_encoding(extracted_files[3])

        # Read the second file into df2
    if len(extracted_files) > 4:
        df5 = read_file_with_encoding(extracted_files[4])


    # Define the new column names directly
    new_column_names = ['Wind Farm','Device','Category','Event','Subevent / Categorization','Start Date','End Date','Duration','Categorization description']

    # Read the extracted files into DataFrames
    df1 = pd.DataFrame()  # Initialize empty DataFrame
    df2 = pd.DataFrame()  # Initialize empty DataFrame
    df3 = pd.DataFrame()  # Initialize empty DataFrame
    df4 = pd.DataFrame()
    df5 = pd.DataFrame()

    if len(extracted_files) > 0:
        # Read the first file into df1
        first_file_path = extracted_files[0]
        if first_file_path.endswith('.xlsx'):
            df1 = pd.read_excel(first_file_path)
        elif first_file_path.endswith('.csv'):
            df1 = pd.read_csv(first_file_path, encoding='latin1', delimiter=';')  # Adjust delimiter if needed

    if len(extracted_files) > 1:
        # Read the second file into df2
        second_file_path = extracted_files[1]
        if second_file_path.endswith('.xlsx'):
            df2 = pd.read_excel(second_file_path)
        elif second_file_path.endswith('.csv'):
            df2 = pd.read_csv(second_file_path, encoding='latin1', delimiter=';')  # Adjust delimiter if needed

    if len(extracted_files) > 2:
        # Read the first file into df1
        first_file_path = extracted_files[2]
        if first_file_path.endswith('.xlsx'):
            df3 = pd.read_excel(first_file_path)
        elif first_file_path.endswith('.csv'):
            df3 = pd.read_csv(first_file_path, encoding='latin1', delimiter=';')  # Adjust delimiter if needed

    if len(extracted_files) > 3:
        # Read the first file into df1
        first_file_path = extracted_files[3]
        if first_file_path.endswith('.xlsx'):
            df4 = pd.read_excel(first_file_path)
        elif first_file_path.endswith('.csv'):
            df4 = pd.read_csv(first_file_path, encoding='latin1', delimiter=';')  # Adjust delimiter if needed

    if len(extracted_files) > 4:
        # Read the first file into df1
        first_file_path = extracted_files[4]
        if first_file_path.endswith('.xlsx'):
            df5 = pd.read_excel(first_file_path)
        elif first_file_path.endswith('.csv'):
            df5 = pd.read_csv(first_file_path, encoding='latin1', delimiter=';')  # Adjust delimiter if needed
            

    # Rename columns for df1 if it has data
    if not df1.empty and len(df1.columns) == len(new_column_names):
        df1.columns = new_column_names

    # Rename columns for df2 if it has data
    if not df2.empty and len(df2.columns) == len(new_column_names):
        df2.columns = new_column_names

    # Rename columns for df2 if it has data
    if not df3.empty and len(df3.columns) == len(new_column_names):
        df3.columns = new_column_names

    # Rename columns for df2 if it has data
    if not df4.empty and len(df4.columns) == len(new_column_names):
        df4.columns = new_column_names

    # Rename columns for df2 if it has data
    if not df5.empty and len(df5.columns) == len(new_column_names):
        df5.columns = new_column_names

    # Combine DataFrames
    df_combined = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)


    # # Define robust function to parse and reformat date
    # def reformat_date_safe(date_str):
    #     if pd.isna(date_str):
    #         return date_str  # Skip NaN values
    #     date_str = str(date_str).strip()
    #     try:
    #         dt = datetime.strptime(date_str, '%Y:%m:%d,%H:%M:%S')
    #         return dt.strftime('%d:%m:%Y, %H:%M:%S')
    #     except ValueError:
    #         return date_str  # Skip malformed values

    # # Ensure column names are exact
    # df_combined['Wind Farm'] = df_combined['Wind Farm'].astype(str).str.strip()
    # df_combined['Start Date'] = df_combined['Start Date'].astype(str).str.strip()

    # # Apply only for 'TAGGUPARTHY' rows
    # mask = df_combined['Wind Farm'] == 'TAGGUPARTHY'
    # df_combined.loc[mask, 'Start Date'] = df_combined.loc[mask, 'Start Date'].apply(reformat_date_safe)



    # Replace 'DESCOPE' with 'KADAMBUR' in the 'Wind Farm' column
    if 'Wind Farm' in df_combined.columns:
        df_combined['Wind Farm'] = df_combined['Wind Farm'].replace('DESCOPE', 'KADAMBUR')

    # Replace 'DESCOPE' with 'KADAMBUR' in the 'Wind Farm' column
    if 'Wind Farm' in df_combined.columns:
        df_combined['Wind Farm'] = df_combined['Wind Farm'].replace('TAGGUPARTHY', 'TGP1')

    # Replace 'DESCOPE' with 'KADAMBUR' in the 'Wind Farm' column
    if 'Wind Farm' in df_combined.columns:
        df_combined['Wind Farm'] = df_combined['Wind Farm'].replace('TAGGUPARTHY II', 'TGP2')

    # Create DataFrames based on the 'Category' column
    df_alarm = df_combined[df_combined['Category'] == 'Alarm']
    df_warning = df_combined[df_combined['Category'] == 'Warning']

    # Remove rows where 'Event' column has the value '203838 Device not synchronized alarm'
    event_value_to_remove = '203838 Device not synchronized alarm'
    df_alarm = df_alarm[df_alarm['Event'] != event_value_to_remove]
    df_warning = df_warning[df_warning['Event'] != event_value_to_remove]

    # Convert 'Duration' column to hh:mm:ss
    def convert_to_hhmmss(duration):
        try:
            # If 'Duration' is already in hh:mm:ss format, it should work
            if isinstance(duration, str) and '.' in duration:
                # Strip milliseconds
                duration = duration.split('.')[0]
            return pd.to_datetime(duration, format='%H:%M:%S').strftime('%H:%M:%S')
        except:
            # Handle cases where 'Duration' might not be in the expected format
            parts = duration.split(':')
            if len(parts) == 3:  # Assume format hh:mm:ss
                return duration.split('.')[0]
            elif len(parts) == 2:  # Assume format mm:ss
                return '00:' + duration
            elif len(parts) == 1:  # Assume total seconds
                seconds = int(parts[0])
                return str(seconds // 3600).zfill(2) + ':' + str((seconds % 3600) // 60).zfill(2) + ':' + str(seconds % 60).zfill(2)
            return '00:00:00'

    # Apply conversion to 'Duration' column
    df_alarm['Duration'] = df_alarm['Duration'].apply(convert_to_hhmmss)
    df_warning['Duration'] = df_warning['Duration'].apply(convert_to_hhmmss)



        # Apply the pivot table to df_alarm
    pivot_df_alarm = df_alarm.pivot_table(
        index=['Wind Farm', 'Device', 'Category', 'Event'],
        values=['Start Date', 'Duration'],
        aggfunc={
            'Start Date': 'count',
            'Duration': lambda x: pd.to_timedelta(x).sum()
        }
    ).reset_index()

    # c1 = ['Wind Farm', 'Device', 'Category', 'Event','Error Count','Duration']
    # pivot_df_alarm.columns = c1

    # Format the Duration column to hh:mm:ss
    pivot_df_alarm['Duration'] = pivot_df_alarm['Duration'].apply(lambda x: str(x).split()[2] if pd.notna(x) else '00:00:00')

    pivot_df_alarm = pivot_df_alarm.rename(columns={'Start Date': 'Error Count'})

    pivot_df_alarm['Remarks'] = ''

    pivot_df_alarm = pivot_df_alarm[ [ 'Wind Farm','Device','Category','Event','Error Count','Duration','Remarks' ] ]


    # Define list of valid TVS device names
    tvs_devices = [f'TVS{i}' for i in range(1, 15)]  # ['TVS1', ..., 'TVS10']

    # # Update site where device is in TVS list
    # df.loc[df['device'].isin(tvs_devices), 'site'] = 'Theni'



    # Update 'site' to 'Theni' if 'TVS' is found in 'device'
    # pivot_df_alarm.loc[pivot_df_alarm['Device'].isin(tvs_devices), 'Wind farm'] = 'THENI'

    # Replace 'Kadambur' with 'Theni' ONLY where device is TVS1 to TVS10
    pivot_df_alarm.loc[(pivot_df_alarm['Device'].isin(tvs_devices)) & (pivot_df_alarm['Wind Farm'] == 'KADAMBUR'), 'Wind Farm'] = 'THENI'
    # Sort the DataFrame by the 'Count' column in descending order

    pivot_df_alarm = pivot_df_alarm.sort_values(by='Error Count', ascending=False)



    # Apply the pivot table to df_warning
    pivot_df_warning = df_warning.pivot_table(
        index=['Wind Farm', 'Device', 'Category', 'Event'],
        values=['Start Date', 'Duration'],
        aggfunc={
            'Start Date': 'count',
            'Duration': lambda x: pd.to_timedelta(x).sum()
        }
    ).reset_index()

    # Format the Duration column to hh:mm:ss
    pivot_df_warning['Duration'] = pivot_df_warning['Duration'].apply(lambda x: str(x).split()[2] if pd.notna(x) else '00:00:00')


    pivot_df_warning = pivot_df_warning.rename(columns={'Start Date': 'Warning Count'})

    pivot_df_warning['Remarks'] = ''

    pivot_df_warning = pivot_df_warning[ [ 'Wind Farm','Device','Category','Event','Warning Count','Duration','Remarks' ] ]



    # Replace 'Kadambur' with 'Theni' ONLY where device is TVS1 to TVS10
    pivot_df_warning.loc[(pivot_df_warning['Device'].isin(tvs_devices)) & (pivot_df_warning['Wind Farm'] == 'KADAMBUR'), 'Wind Farm'] = 'THENI'



    # Sort the DataFrame by the 'Count' column in descending order

    pivot_df_warning = pivot_df_warning.sort_values(by='Warning Count', ascending=False)


    # c2 = ['Wind Farm', 'Device', 'Category', 'Event','Warning Count','Duration']
    # pivot_df_warning.columns = c2

    # Verify the pivot tables
    print("Pivot Table for df_alarm:")
    print(pivot_df_alarm.head())

    print("Pivot Table for df_warning:")
    print(pivot_df_warning.head())


        # Apply cleaning to relevant columns
    # pivot_df_alarm['Event'] = pivot_df_alarm['Event'].apply(clean_text)
    # pivot_df_warning['Event'] = pivot_df_warning['Event'].apply(clean_text)

    # pivot_df_alarm = pivot_df_alarm.rename(columns={'Start Date': 'Error_Count'})
    # pivot_df_warning = pivot_df_warning.rename(columns={'Start Date': 'Error_Count'})

    # # Sort the DataFrame by the 'Count' column in descending order
    # pivot_df_alarm = pivot_df_alarm.sort_values(by='Error_Count', ascending=False)

    # # Sort the DataFrame by the 'Count' column in descending order
    # pivot_df_warning = pivot_df_warning.sort_values(by='Error_Count', ascending=False)

    # exist_df = exist_df.rename(columns={'Column_0': 'Error_time'})



    # Database connection details
    server = r'RENOM-PC2\SQLEXPRESS'
    database = 'fault_analysis'
    username = ''
    password = ''

    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'CHARSET=UTF8'
    )
    conn = pyodbc.connect(conn_str)

    def connect_to_database():
        return pyodbc.connect(conn_str)

    def close_connection(conn):
        conn.close()

    # Define a function to check if a record exists based on specific columns
    def record_exists(cursor, table_name, wind_farm, device, category, event, start_date, end_date, duration):
        query = f"""
        SELECT COUNT(*) FROM {table_name}
        WHERE Wind_Farm = ? AND Device = ? AND Category = ? AND Event = ?
        AND Start_Date = ? AND End_Date = ? AND Duration = ?
        """
        cursor.execute(query, (wind_farm, device, category, event, start_date, end_date, duration))
        return cursor.fetchone()[0] > 0

    # Define a function to insert records
    def insert_record(cursor, table_name, wind_farm, device, category, event, start_date, end_date, duration):
        query = f"""
        INSERT INTO {table_name} (Wind_Farm, Device, Category, Event, Start_Date, End_Date, Duration)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        try:
            cursor.execute(query, (wind_farm, device, category, event, start_date, end_date, duration))
        except pyodbc.Error as e:
            print(f"Error during insert: {e}")

    def convert_to_sql_datetime_format(date_str):
        try:
            # First, parse the date string in the format 'dd:mm:yyyy HH:MM:SS'
            dt = pd.to_datetime(date_str, format='%d:%m:%Y %H:%M:%S')
            # Convert it to the SQL Server compatible format 'YYYY-MM-DD HH:MM:SS'
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error converting date {date_str}: {e}")
            return None

    # Apply the conversion to 'Start Date' and 'End Date' columns
    df_alarm['Start Date'] = df_alarm['Start Date'].apply(convert_to_sql_datetime_format)
    df_alarm['End Date'] = df_alarm['End Date'].apply(convert_to_sql_datetime_format)
    df_warning['Start Date'] = df_warning['Start Date'].apply(convert_to_sql_datetime_format)
    df_warning['End Date'] = df_warning['End Date'].apply(convert_to_sql_datetime_format)

    # Define a function to clean text fields and handle encoding issues
    def clean_text(text):
        if isinstance(text, str):
            text = text.replace('?', '')  # Replace any invalid characters
            text = text.encode('latin1').decode('utf-8', 'ignore')  # Fix encoding issues
        return text


    # with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.precision', 3,
    #                     'expand_frame_repr', False):
    #     print(df_alarm)

    def process_and_upload(df, table_name):
        if not df.empty:
            conn = connect_to_database()
            cursor = conn.cursor()

            # Start a transaction
            conn.autocommit = False

            try:
                # Process each record
                for index, row in df.iterrows():
                    try:
                        print(f"Processing record {index} for {table_name}: {row.to_dict()}")  # Print out the row data for debugging

                        # Extract relevant fields
                        wind_farm = row['Wind Farm']
                        device = row['Device']
                        category = row['Category']
                        event = row['Event']
                        start_date = row['Start Date']
                        end_date = row['End Date']
                        duration = row['Duration']

                        # Check if the exact record exists in the SQL table
                        if not record_exists(cursor, table_name, wind_farm, device, category, event, start_date, end_date, duration):
                            # If the record doesn't exist, insert it
                            insert_record(cursor, table_name, wind_farm, device, category, event, start_date, end_date, duration)
                    
                    except Exception as e:
                        print(f"Error processing record {index} for {table_name}: {e}")

                # Commit the transaction
                conn.commit()
            except Exception as e:
                print(f"Transaction error for {table_name}: {e}")
                conn.rollback()
            finally:
                # Close the connection
                cursor.close()
                conn.close()
        else:
            print(f"No data available to process for {table_name}.")
            
    # Assuming df_alarm and df_warning are defined and contain the data
    process_and_upload(df_alarm, 'Gamesa_Errors')
    process_and_upload(df_warning, 'Gamesa_Warning')

    parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
    # Format the date for the filename as dd-mm-yyyy
    formatted_filename_date = parsed_date.strftime('%d-%m-%Y')

    # Create the file name in the required format
    file_name1 = f"{formatted_filename_date} Gamesa Daily Error Report.xlsx"
    file_name2 = f"{formatted_filename_date} Gamesa Daily Warning Report.xlsx"

    # Define the file path using the function name
    # file_name = f"{'Gamesa Daily Error Report'}.xlsx"

    # Combine the folder path and file name
    save_path1 = os.path.join(folder_path, file_name1)
    save_path2 = os.path.join(folder_path, file_name2)

# Save the DataFrames to the specified Excel file
    with pd.ExcelWriter(save_path1) as writer:
        # df1.to_excel(writer, sheet_name='df1', index=False)
        # df2.to_excel(writer, sheet_name='df2', index=False)
        # df3.to_excel(writer, sheet_name='df3', index=False)
        # df4.to_excel(writer, sheet_name='df4', index=False)
        # df_combined.to_excel(writer, sheet_name='combined', index=False)
        # df_alarm.to_excel(writer, sheet_name='Alarm Data', index=False)
        # df_warning.to_excel(writer, sheet_name='Warning Data', index=False)
        pivot_df_alarm.to_excel(writer, sheet_name='Error Remarks Sheet', index=False)
        # pivot_df_warning.to_excel(writer, sheet_name='Warning Remarks Sheet', index=False)

# Save the DataFrames to the specified Excel file
    with pd.ExcelWriter(save_path2) as writer:
        # df1.to_excel(writer, sheet_name='df1', index=False)
        # df2.to_excel(writer, sheet_name='df2', index=False)
        # df3.to_excel(writer, sheet_name='df3', index=False)
        # df4.to_excel(writer, sheet_name='df4', index=False)
        # df_combined.to_excel(writer, sheet_name='combined', index=False)
        # df_alarm.to_excel(writer, sheet_name='Alarm Data', index=False)
        # df_warning.to_excel(writer, sheet_name='Warning Data', index=False)
        # pivot_df_alarm.to_excel(writer, sheet_name='Error Remarks Sheet', index=False)
        pivot_df_warning.to_excel(writer, sheet_name='Warning Remarks Sheet', index=False)


def inox_tml():


    # Retrieve start and end date and folder path for processing
    start_date_str = stored_start_date  # Start date
    end_date_str = stored_end_date  # End date
    print(f'start_date_str:', {start_date_str})
    print(f'end_date_str:', {end_date_str})
    folder_path = stored_folder_link

    # Convert the start and end date strings back to datetime objects
    if start_date_str and end_date_str:
        parsed_start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        parsed_end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Simulate some processing
        st.write(f"Inox Error Module Executed!")
        st.write(f"Parsed Start Date: {parsed_start_date.strftime('%Y-%m-%d')}")
        st.write(f"Parsed End Date: {parsed_end_date.strftime('%Y-%m-%d')}")
        st.write(f"Folder Path: {folder_path}")

        # print(f'parsed_start_date Date:', {parsed_start_date})
        # print(f'parsed_end_date Date:', {parsed_end_date}')
    else:
        print("Error: Start or end date not provided.")
        st.error("Please provide both start and end dates.")

    # Check if folder_path and date(s) are not None
    if folder_path is None:
        print("Error: No folder link provided.")
        st.error("Please provide a valid folder link.")


    # date_str = stored_date
    # print(f'date_str Date:', {date_str})
    # folder_path = stored_folder_link

    # # Convert the date string back to a datetime object
    # parsed_date = datetime.strptime(date_str, '%Y-%m-%d')

    # # Simulate some processing
    # st.write(f"Inox Error Module Executed!")
    # st.write(f"Parsed Date: {parsed_date.strftime('%Y-%m-%d')}")
    # st.write(f"Folder Path: {folder_path}")

    # print(f'parsed_date Date:', {parsed_date})

    # # Check if folder_path and date are not None
    # if folder_path is None:
    #     print("Error: No folder link provided.")
    #     return
    # if date_str is None:
    #     print("Error: No date provided.")
    #     return

    # Define the lists of turbines
    EDF = [ "GK03", "GK04", "KL01", "KL02", "KL03", "KL04", "KL05", "KL06", "SP01", "SP02", "SP03", "SP04", "SP05", "SP06","SP07", "SP08", "SP09", "SP10" ]
    ZR = [ "ZR01", "ZR03", "ZR06", "ZR07", "ZR08", "ZR09", "ZR10" ]
    Hero = [ "HC02", "HC03", "HC05", "HC06", "HC11", "HC14", "HC15", "HC16", "HC17", "HC18", "HC19", "HC20", "SBT20","SBT40", "SBT52", "SBT91", "DANT142", "DANT143", "DANT148", "DANT149", "DANT150", "DANT151", "DANT152","DANT153", "DANT241", "DANT289", "KBS05", "KBS06", "KBS07", "KBS10", "KBS34", "KBS47", "KBS65", "KBS66","KBS67", "KBS68", "KBS70", "KBS73", "KBS74", "KBS75", "KBS78", "KBS79", "KBS80", "KBS81", "KBS82", "KBS83" ]
    LGE = [ "DANT 100", "DANT 101", "DANT 102", "DANT 105", "DANT 106", "DANT 107", "DANT 108", "DANT 109", "DANT 110","DANT 111", "DANT 112", "DANT 113", "DANT 114", "DANT 115", "DANT 116", "DANT 117", "DANT 16", "DANT 211","DANT 212", "DANT 229", "DANT 232", "DANT 233", "DANT 28", "DANT 299", "DANT 44", "DANT 45", "DANT 46","DANT 47", "DANT 48", "DANT 49", "DANT 57", "DANT 91", "DANT 94", "DANT 95", "DANT 96", "DANT 104", "DANT 118","DANT 120", "DANT 139", "DANT 140", "DANT 17", "DANT 18", "DANT 19", "DANT 20", "DANT 21", "DANT 22","DANT 222", "DANT 223", "DANT 225", "DANT 226", "DANT 227", "DANT 228", "DANT 23", "DANT 42", "DANT 52","DANT 53", "DANT 62", "DANT 63", "DANT 64", "DANT 65", "DANT 66", "DANT 67", "DANT 68", "DANT 90", "DANT 92","DANT 98", "DANT 99" ]
    Oil_India = [ "DANT130", "DANT131", "DANT132", "DANT134", "DANT161", "DANT162", "DANT163", "DANT164", "DANT165","DANT167", "DANT168", "DANT169", "DANT170", "DANT242", "DANT245", "DANT246", "DANT247", "DANT248","DANT250", "DANT251", "DANT252", "DANT253", "DANT254", "DANT282", "DANT285", "DANT79", "DANT80" ]
    Tata = [ "DANT124", "DANT126", "DANT128", "DANT129", "DANT135", "DANT136", "DANT145", "DANT243", "DANT244", "DANT281","DANT383", "DANT385", "DANT386", "KBS26", "KBS27", "KBS33", "KBS51", "KBS52", "KBS53" ]
    Atria = [ "SVRT100", "SVRT101", "SVRT102", "SVRT116", "SVRT124", "SVRT129", "SVRT134", "SVRT22", "SVRT23","SVRT31", "SVRT49", "SVRT56", "SVRT68", "SVRT84", "SVRT87", "SVRT93" ]
    Torrent = [ "DANT123", "DANT224", "DANT230", "DANT231", "DANT30", "DANT31", "DANT32", "DANT33", "DANT36", "DANT54","DANT81", "DANT83", "GGM02", "GGM03", "GGM04", "GGM09", "GGM10", "GGM109", "GGM110", "GGM117", "GGM126","GGM133", "GGM141", "GGM16", "GGM19", "NPYP 57", "NPYP 85", "NPYP 87", "NPYP3 113", "NPYP3 13", "NPYP3 14","NPYP3 142", "NPYP3 15", "NPYP3 155", "NPYP3 156", "NPYP3 158", "NPYP3 173", "NPYP3 174", "NPYP3 175","NPYP3 176", "NPYP3 28", "NPYP3 43", "NPYP3 44", "RJ4T43", "RJ8T001", "RJ8T002", "RJ8T88", "RJ8T98","RJ9T002", "RJ9T003", "RJ9T004", "RJ9T007", "RJ9T21","RJ9T22", "RJ9T36", "RJ9T38", "RJ9T41", "RJ9T43","RJ9T46", "RJ9T58", "RJ9T66", "RJ9T73", "RJ9T86","RJ9T88", "RJ9T90", "RJ9T101", "RJ9T105", "RJPT006","RJPT124", "RJPT154", "RJPT155", "RJPT160", "RJPT162","RJPT165", "RJPT166", "RJPT168", "RJPT170", "RJPT175" ]
    BG_WIND = ["KHD08", "KHD114", "KHD13", "KHD14", "KHD19", "KHD20", "KHD31", "KHD32", "KHD33", "KHD34",'BHT01','BHT02','BHT05','BHT08','BHT13','BHT18','BHT19']
    JWEPL = ['MVT5','MVT10','MVT11','MVT45','MVT61','MVT62','MVT63','MVT84','MVT95','MV2T2','MV2T3','MV2T15','MV2T17','MV2T21','MV2T24','MV2T28','MV2T37','MV2T41','MV2T42','MV2T48','GME1','GME2','GME3','GME5','GME6']
    JTPL = ['VALT02','VALT03','VALT04','VALT05','VALT06','TAL01','TAL07']

    Kalorana = [ "GK03", "GK04", "KL01", "KL02", "KL03", "KL04", "KL05", "KL06", "SP01", "SP02", "SP03", "SP04", "SP05","SP06", "SP07", "SP08", "SP09", "SP10" ]
    Khanapur = [ "HC02", "HC03", "HC05", "HC06", "HC11", "HC14", "HC15", "HC16", "HC17", "HC18", "HC19", "HC20", "SBT20","SBT40", "SBT52", "SBT91" ]
    Mahidad = [ "GGM02", "GGM03", "GGM04", "GGM09", "GGM10", "GGM109", "GGM110", "GGM117", "GGM126", "GGM133", "GGM141","GGM16", "GGM19" ]
    Tadipatri = [ "ZR01", "ZR03", "ZR06", "ZR07", "ZR08", "ZR09", "ZR10" ]
    Dangri = [ "DANT142", "DANT143", "DANT148", "DANT149", "DANT150", "DANT151", "DANT152", "DANT153", "DANT241", "DANT289","KBS05", "KBS06", "KBS07", "KBS10", "KBS34", "KBS47", "KBS65", "KBS66", "KBS67", "KBS68", "KBS70", "KBS73","KBS74", "KBS75", "KBS78", "KBS79", "KBS80", "KBS81", "KBS82", "KBS83", "DANT 104", "DANT 118", "DANT 120","DANT 139", "DANT 140", "DANT 17", "DANT 18", "DANT 19", "DANT 20", "DANT 21", "DANT 22", "DANT 222","DANT 223", "DANT 225", "DANT 226", "DANT 227", "DANT 228", "DANT 23", "DANT 42", "DANT 52", "DANT 53","DANT 62", "DANT 63", "DANT 64", "DANT 65", "DANT 66", "DANT 67", "DANT 68", "DANT 90", "DANT 92", "DANT 98","DANT 99", "DANT 100", "DANT 101", "DANT 102", "DANT 105", "DANT 106", "DANT 107", "DANT 108", "DANT 109","DANT 110", "DANT 111", "DANT 112", "DANT 113", "DANT 114", "DANT 115", "DANT 116", "DANT 117", "DANT 16","DANT 211", "DANT 212", "DANT 229", "DANT 232", "DANT 233", "DANT 28", "DANT 299", "DANT 44", "DANT 45","DANT 46", "DANT 47", "DANT 48", "DANT 49", "DANT 57", "DANT 91", "DANT 94", "DANT 95", "DANT 96", "DANT130","DANT131", "DANT132", "DANT134", "DANT161", "DANT162", "DANT163", "DANT164", "DANT165", "DANT167", "DANT168","DANT169", "DANT170", "DANT242", "DANT245", "DANT246", "DANT247", "DANT248", "DANT250", "DANT251", "DANT252","DANT253", "DANT254", "DANT282", "DANT285", "DANT79", "DANT80", "DANT124", "DANT126", "DANT128", "DANT129","DANT135", "DANT136", "DANT145", "DANT243", "DANT244", "DANT281", "DANT383", "DANT385", "DANT386", "KBS26","KBS27", "KBS33", "KBS51", "KBS52", "KBS53", "DANT123", "DANT224", "DANT230", "DANT231", "DANT30", "DANT31","DANT32", "DANT33", "DANT36", "DANT54", "DANT81", "DANT83","KHD08", "KHD114", "KHD13", "KHD14", "KHD19", "KHD20", "KHD31", "KHD32", "KHD33", "KHD34" ]
    Nipaniya = [ "NPYP 57", "NPYP 85", "NPYP 87", "NPYP3 113", "NPYP3 13", "NPYP3 14", "NPYP3 142", "NPYP3 15", "NPYP3 155","NPYP3 156", "NPYP3 158", "NPYP3 173", "NPYP3 174", "NPYP3 175", "NPYP3 176", "NPYP3 28", "NPYP3 43","NPYP3 44" ]
    Savarkundla = [ "SVRT100", "SVRT101", "SVRT102", "SVRT116", "SVRT124", "SVRT129", "SVRT134", "SVRT22", "SVRT23","SVRT31", "SVRT49", "SVRT56", "SVRT68", "SVRT84", "SVRT87", "SVRT93" ]
    Rojmal = ["RJ4T43", "RJ8T001", "RJ8T002", "RJ8T88", "RJ8T98","RJ9T002", "RJ9T003", "RJ9T004", "RJ9T007", "RJ9T21","RJ9T22", "RJ9T36", "RJ9T38", "RJ9T41", "RJ9T43","RJ9T46", "RJ9T58", "RJ9T66", "RJ9T73", "RJ9T86","RJ9T88", "RJ9T90", "RJ9T101", "RJ9T105", "RJPT006","RJPT124", "RJPT154", "RJPT155", "RJPT160", "RJPT162","RJPT165", "RJPT166", "RJPT168", "RJPT170", "RJPT175"]
    # Rojmal = [ "RJ4T 43", "RJ8T 001", "RJ8T 002", "RJ8T 88", "RJ8T98", "RJ9T 002", "RJ9T 003", "RJ9T 004", "RJ9T 007","RJ9T 101", "RJ9T 105", "RJ9T 21", "RJ9T 22", "RJ9T 36", "RJ9T 38", "RJ9T 41", "RJ9T 43", "RJ9T 46","RJ9T 58", "RJ9T 66", "RJ9T 73", "RJ9T 86", "RJ9T 88", "RJ9T 90", "RJPT 006", "RJPT 124", "RJPT 154","RJPT 155", "RJPT 162", "RJPT 165", "RJPT 166", "RJPT 168", "RJPT 170", "RJPT 175", "RJPT160" ]
    Bhendewade = ['BHT01','BHT02','BHT05','BHT08','BHT13','BHT18','BHT19']
    Jath = ['MVT5','MVT10','MVT11','MVT45','MVT61','MVT62','MVT63','MVT84','MVT95','MV2T2','MV2T3','MV2T15','MV2T17','MV2T21','MV2T24','MV2T28','MV2T37','MV2T41','MV2T42','MV2T48','GME1','GME2','GME3','GME5','GME6']
    CkPalli = ['VALT02','VALT03','VALT04','VALT05','VALT06','TAL01','TAL07']


    max_length = max(len(Kalorana), len(Khanapur), len(Mahidad), len(Tadipatri), len(Dangri), len(Nipaniya),len(Savarkundla), len(Rojmal),len(Bhendewade),len(Jath), len(CkPalli))
    Kalorana += [ '' ] * (max_length - len(Kalorana))
    Khanapur += [ '' ] * (max_length - len(Khanapur))
    Mahidad += [ '' ] * (max_length - len(Mahidad))
    Tadipatri += [ '' ] * (max_length - len(Tadipatri))
    Dangri += [ '' ] * (max_length - len(Dangri))
    Nipaniya += [ '' ] * (max_length - len(Nipaniya))
    Savarkundla += [ '' ] * (max_length - len(Savarkundla))
    Rojmal += [ '' ] * (max_length - len(Rojmal))
    Bhendewade += [ '' ] * (max_length - len(Bhendewade))
    Jath += [ '' ] * (max_length - len(Jath))
    CkPalli += [ '' ] * (max_length - len(CkPalli))

    # Ensure all lists have the same length
    max_length1 = max(len(EDF), len(ZR), len(Hero), len(LGE), len(Oil_India), len(Tata), len(Atria), len(Torrent),len(JWEPL), len(JTPL))
    EDF += [ '' ] * (max_length1 - len(EDF))
    ZR += [ '' ] * (max_length1 - len(ZR))
    Hero += [ '' ] * (max_length1 - len(Hero))
    LGE += [ '' ] * (max_length1 - len(LGE))
    Oil_India += [ '' ] * (max_length1 - len(Oil_India))
    Tata += [ '' ] * (max_length1 - len(Tata))
    Atria += [ '' ] * (max_length1 - len(Atria))
    Torrent += [ '' ] * (max_length1 - len(Torrent))
    BG_WIND += [ '' ] * (max_length1 - len(BG_WIND))
    JWEPL += [ '' ] * (max_length1 - len(JWEPL))
    JTPL += [ '' ] * (max_length1 - len(JTPL))

    # Create the DataFrame
    customer_name = pd.DataFrame({'EDF': EDF,'ZR': ZR,'Hero': Hero,'LGE': LGE,'Oil India': Oil_India,'Tata': Tata,'Atria': Atria,'Torrent': Torrent,'BG_WIND': BG_WIND,'JWEPL':JWEPL,'JTPL':JTPL})

    Site_name = pd.DataFrame({'Kalorana': Kalorana,'Khanapur': Khanapur,'Mahidad': Mahidad,'Tadipatri': Tadipatri,'Dangri': Dangri,'Nipaniya': Nipaniya,'Savarkundla': Savarkundla,'Rojmal': Rojmal,'Bhendewade' : Bhendewade,'Jath':Jath,'CkPalli':CkPalli})
    # Define your renaming function
    def renaming_fun(x):
        x = str(x)
        if "C11_CON/.Glo.Con.LscIgbTemMax - AVE [C]" in x or "Line-side IGBT max.temp. - AVE [C]" in x or "LSC IGBT temp. - AVE [C]" in x:
            return "LSC IGBT Temp[AVE]"
        elif "C11_CON/.Glo.Con.LscIgbTemMax - MAX [C]" in x or "Line-side IGBT max.temp. - MAX [C]" in x or "LSC IGBT temp. - MAX [C]" in x:
            return "LSC IGBT Temp[MAX]"

        elif "C11_CON/.Glo.Con.GscIgbTemMax - AVE [C]" in x or "Gen-side IGBT max.temp. - AVE [C]" in x or "GSC IGBT temp. - AVE [C]" in x:
            return "GSC IGBT Temp[AVE]"
        elif "C11_CON/.Glo.Con.GscIgbTemMax - MAX [C]" in x or "Gen-side IGBT max.temp. - MAX [C]" in x or "GSC IGBT temp. - MAX [C]" in x:
            return "GSC IGBT Temp[MAX]"

        elif "Energy production 10min - SUM [kWh]" in x or "@Cnt10mProPow - SUM [kWh]" in x or "Energy production 10min - SUM [kWh]" in x:
            return "Total Day Power Production[kWh]"
        elif "C11/.Glo.Gri.PowAct - AVE [kW]" in x or "Active power - AVE [kW]" in x or "C11_5/.Glo.Gri.PowActNet - AVE [kW]" in x:
            return "Active power - AVE [kW]"
        
        elif "C11/.Glo.Gri.PowAct - MAX [kW]" in x or "Active power - MAX [kW]" in x or "C11_5/.Glo.Gri.PowActNet - MAX [kW]" in x:
            return "Active power - MAX [kW]"


        return x


    def find_xlsx_files(folder_path):
        xlsx_files = []
        # Traverse the folder recursively
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.xlsx'):
                    xlsx_files.append(os.path.join(root, file))
        return xlsx_files

    def read_xlsx_to_dataframe(xlsx_file):
        try:
            df = pd.read_excel(xlsx_file)
            return df
        except Exception as e:
            # print(f'Error reading {xlsx_file}: {str(e)}')
            return None

    def make_columns_unique(df):
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
        df.columns = cols
        return df

    def unzip_all_files_in_folder(folder_path):
        # Get list of all files in the folder
        files = os.listdir(folder_path)
        
        # Filter out only the zip files
        zip_files = [file for file in files if file.endswith('.zip')]
        
        all_dataframes = []  # List to store all DataFrames
        
        for zip_file in zip_files:
            zip_file_path = os.path.join(folder_path, zip_file)
            # Create a new folder with the same name as the zip file (without .zip)
            new_folder_name = os.path.splitext(zip_file)[0]
            new_folder_path = os.path.join(folder_path, new_folder_name)
            os.makedirs(new_folder_path, exist_ok=True)
            
            # Unzip the file into the new folder
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(new_folder_path)
            
            # Find all .xlsx files in the extracted folder
            xlsx_files = find_xlsx_files(new_folder_path)
            
            # Read each .xlsx file into a DataFrame
            for xlsx_file in xlsx_files:
                df = read_xlsx_to_dataframe(xlsx_file)
                if df is not None:
                    # Transpose the DataFrame
                    df = df.transpose()
                    
                    # Reset the index to bring 'Variable' row back as a header
                    df.reset_index(inplace=True)
                    new_header = df.iloc[0] # grab the first row for the header
                    df = df[1:] # take the data less the header row
                    df.columns = new_header # set the header row as the df header
                    
                    # Ensure unique column names
                    df = make_columns_unique(df)
                    
                    # Apply renaming_fun to column names
                    df.rename(columns=renaming_fun, inplace=True)
                    
                    # Filter columns based on filter_list
                    filter_list = ["Variable","Log time (Local)","Active power - AVE [kW]","Active power - MAX [kW]","Blade 1 converter internal temperature - AVE [C]","Blade 2 converter internal temperature - AVE [C]","Blade 3 converter internal temperature - AVE [C]","Converter cab. 1 temp. - AVE [C]","Converter cab. 1 temp. - MAX [C]","Cooling plate temp. - AVE [C]","Cooling plate temp. - MAX [C]","Gearbox oil heater temp. - AVE [C]","Gearbox oil heater temp. - MAX [C]",
                    "Gearbox oil tank temp. - AVE [C]","Gearbox oil tank temp. - MAX [C]","Gearbox rotor bearing temp. - AVE [C]","Gearbox rotor bearing temp. - MAX [C]","Gearbox shaft bearing temp. 1 - AVE [C]","Gearbox shaft bearing temp. 1 - MAX [C]","Gearbox shaft bearing temp. 2 - AVE [C]","Gearbox shaft bearing temp. 2 - MAX [C]","Gearbox shaft bearing temp. 3 - AVE [C]","Gearbox shaft bearing temp. 3 - MAX [C]","GSC IGBT Temp[AVE]",
                    "GSC IGBT Temp[MAX]","Gen. bearing drive temp. - AVE [C]","Gen. bearing drive temp. - MAX [C]","Gen. bearing non-drive temp. - AVE [C]","Gen. bearing non-drive temp. - MAX [C]","Gen. water inlet temp. - AVE [C]","Gen. water inlet temp. - MAX [C]","Gen. winding [U] temp. - AVE [C]","Gen. winding [U] temp. - MAX [C]","Gen. winding [V] temp. - AVE [C]","Gen. winding [V] temp. - MAX [C]","Gen. winding [W] temp. - AVE [C]","Gen. winding [W] temp. - MAX [C]","Generator choke temp. - AVE [C]",
                    "Generator choke temp. - MAX [C]","Hub cab. 1 temp. - AVE [C]","Hub cab. 2 temp. - AVE [C]","Hub cab. 3 temp. - AVE [C]","Line choke temp. - AVE [C]","Line choke temp. - MAX [C]","LSC IGBT Temp[AVE]","LSC IGBT Temp[MAX]","Nacelle cab. 1 temp. - AVE [C]","Nacelle cab. 1 temp. - MAX [C]",
                    "Temperature inside converter cabinet 2 - AVE [C]","Temperature inside converter cabinet 2 - MAX [C]","Tower temp. - AVE [C]","Tower temp. - MAX [C]","Towerbase cab. 1 temp. - AVE [C]","Towerbase cab. 1 temp. - MAX [C]","Wind speed - AVE [m/s]","Wind speed - MAX [m/s]"]
                    
                    df_filtered = df.filter(items=filter_list)
                    
                    # print(f'Transposed, renamed columns, and filtered for {xlsx_file}:')
                    # print(df_filtered.head())  # Print the first few rows of the filtered DataFrame
                    
                    all_dataframes.append(df_filtered)  # Add DataFrame to the list

                    # Extract site name from filename
                    filename = os.path.basename(xlsx_file)
                    parts = filename.split('_')
                    if len(parts) == 2:
                        site_name = parts[0]  # For filenames like 'NPYP 85_11.06.2024'
                    elif len(parts) >= 3:
                        site_name = parts[1]  # For filenames like 'tenminlog_NPYP 85_11.06.2024'
                    else:
                        site_name = filename  # Handle other cases if necessary
                    
                    # Add 'site' column to new_df
                    df_filtered['Turbine'] = site_name


                    # Extract site name from filename
                    filename = os.path.basename(xlsx_file)
                    parts = filename.split('_')
                    if len(parts) == 2:
                        site_name = parts[1]  # For filenames like 'NPYP 85_11.06.2024'
                    elif len(parts) >= 3:
                        site_name = parts[2]  # For filenames like 'tenminlog_NPYP 85_11.06.2024'
                    else:
                        site_name = filename  # Handle other cases if necessary
                    
                    # Add 'site' column to new_df
                    df_filtered['Date'] = site_name

                    # Iterate through the rows of exist_df
                    for index, row in df_filtered.iterrows():
                        turbine = row[ 'Turbine' ]
                        # Iterate through the columns of site_name DataFrame
                        for col in Site_name.columns:
                            # Check if the turbine exists in the current column of site_name
                            if turbine in Site_name[ col ].values:
                                df_filtered.at[ index, 'Site' ] = str(col)  # Add the index name of the column to 'Site' column
                                break  # Break the loop if the turbine is found

                    # Iterate through the rows of df_filtered
                    for index, row in df_filtered.iterrows():
                        turbine = row['Turbine' ]
                        # Iterate through the columns of site_name DataFrame
                        for col in customer_name.columns:
                            # Check if the turbine exists in the current column of site_name
                            if turbine in customer_name[ col ].values:
                                df_filtered.at[ index, 'Customer' ] = str(col)  # Add the index name of the column to 'Site' column
                                break  # Break the loop if the turbine is found                    
                
                    # Function to remove '.xlsx' from a string
                    def remove_extension(date_value):
                        if isinstance(date_value, str) and date_value.endswith('.xlsx'):
                            return date_value.replace('.xlsx', '')
                        return date_value

                    # Apply the function to the 'Date' column
                    df_filtered['Date'] = df_filtered['Date'].apply(remove_extension)  


                    # Function to check if a value is in date format and get week number
                    def get_Week(date_value):
                        try:
                            date_obj = datetime.strptime(date_value, '%d.%m.%Y')  # Adjust format as per your date format
                            return date_obj.isocalendar()[1]  # Get ISO week number
                        except ValueError:
                            return None

                    # Apply the function to the 'Date' column to get week numbers
                    df_filtered['Week'] = df_filtered['Date'].apply(get_Week)

                    # Remove the last row
                    df_filtered.drop(df.index[-1], inplace=True)

                    df_filtered = pd.DataFrame(df_filtered)

                    new_df = pd.DataFrame()

                    new_df = pd.concat([new_df, df_filtered], ignore_index=True)
                
                else:
                    print(f'Failed to read {xlsx_file} into DataFrame.')

            # Concatenate all DataFrames in new_df into a single DataFrame

        new_df = pd.concat(all_dataframes, ignore_index=True)
        final_df = pd.DataFrame(new_df)
        final_df = final_df[ [ "Turbine" ,"Date" ,"Site" ,"Customer" ,"Week", "Variable" ,"Log time (Local)" ,"Active power - AVE [kW]" ,"Active power - MAX [kW]" ,"Blade 1 converter internal temperature - AVE [C]" ,"Blade 2 converter internal temperature - AVE [C]" ,"Blade 3 converter internal temperature - AVE [C]" ,"Converter cab. 1 temp. - AVE [C]" ,"Converter cab. 1 temp. - MAX [C]" ,"Cooling plate temp. - AVE [C]" ,"Cooling plate temp. - MAX [C]" ,"Gearbox oil heater temp. - AVE [C]" ,"Gearbox oil heater temp. - MAX [C]" ,"Gearbox oil tank temp. - AVE [C]" ,"Gearbox oil tank temp. - MAX [C]" ,"Gearbox rotor bearing temp. - AVE [C]" ,"Gearbox rotor bearing temp. - MAX [C]" ,"Gearbox shaft bearing temp. 1 - AVE [C]" ,"Gearbox shaft bearing temp. 1 - MAX [C]" ,"Gearbox shaft bearing temp. 2 - AVE [C]" ,"Gearbox shaft bearing temp. 2 - MAX [C]" ,"Gearbox shaft bearing temp. 3 - AVE [C]" ,"Gearbox shaft bearing temp. 3 - MAX [C]" ,"GSC IGBT Temp[AVE]" ,"GSC IGBT Temp[MAX]" ,"Gen. bearing drive temp. - AVE [C]" ,"Gen. bearing drive temp. - MAX [C]" ,"Gen. bearing non-drive temp. - AVE [C]" ,"Gen. bearing non-drive temp. - MAX [C]" ,"Gen. water inlet temp. - AVE [C]" ,"Gen. water inlet temp. - MAX [C]" ,"Gen. winding [U] temp. - AVE [C]" ,"Gen. winding [U] temp. - MAX [C]" ,"Gen. winding [V] temp. - AVE [C]" ,"Gen. winding [V] temp. - MAX [C]" ,"Gen. winding [W] temp. - AVE [C]" ,"Gen. winding [W] temp. - MAX [C]" ,"Generator choke temp. - AVE [C]" ,"Generator choke temp. - MAX [C]" ,"Hub cab. 1 temp. - AVE [C]" ,"Hub cab. 2 temp. - AVE [C]" ,"Hub cab. 3 temp. - AVE [C]" ,"Line choke temp. - AVE [C]" ,"Line choke temp. - MAX [C]" ,"LSC IGBT Temp[AVE]" ,"LSC IGBT Temp[MAX]" ,"Nacelle cab. 1 temp. - AVE [C]" ,"Nacelle cab. 1 temp. - MAX [C]" ,"Temperature inside converter cabinet 2 - AVE [C]" ,"Temperature inside converter cabinet 2 - MAX [C]" ,"Tower temp. - AVE [C]" ,"Tower temp. - MAX [C]" ,"Towerbase cab. 1 temp. - AVE [C]" ,"Towerbase cab. 1 temp. - MAX [C]" ,"Wind speed - AVE [m/s]" ,"Wind speed - MAX [m/s]"] ]
        
        # List of columns to be modified
        columns_to_modify = ["Active power - AVE [kW]", "Active power - MAX [kW]", "Blade 1 converter internal temperature - AVE [C]", "Blade 2 converter internal temperature - AVE [C]", "Blade 3 converter internal temperature - AVE [C]", "Converter cab. 1 temp. - AVE [C]", "Converter cab. 1 temp. - MAX [C]", "Cooling plate temp. - AVE [C]", "Cooling plate temp. - MAX [C]", "Gearbox oil heater temp. - AVE [C]", "Gearbox oil heater temp. - MAX [C]",   "Gearbox oil tank temp. - AVE [C]", "Gearbox oil tank temp. - MAX [C]", "Gearbox rotor bearing temp. - AVE [C]", "Gearbox rotor bearing temp. - MAX [C]", "Gearbox shaft bearing temp. 1 - AVE [C]", "Gearbox shaft bearing temp. 1 - MAX [C]", "Gearbox shaft bearing temp. 2 - AVE [C]", "Gearbox shaft bearing temp. 2 - MAX [C]", "Gearbox shaft bearing temp. 3 - AVE [C]", "Gearbox shaft bearing temp. 3 - MAX [C]", "GSC IGBT Temp[AVE]", "GSC IGBT Temp[MAX]", "Gen. bearing drive temp. - AVE [C]", "Gen. bearing drive temp. - MAX [C]", "Gen. bearing non-drive temp. - AVE [C]", "Gen. bearing non-drive temp. - MAX [C]", "Gen. water inlet temp. - AVE [C]", "Gen. water inlet temp. - MAX [C]", "Gen. winding [U] temp. - AVE [C]", "Gen. winding [U] temp. - MAX [C]", "Gen. winding [V] temp. - AVE [C]", "Gen. winding [V] temp. - MAX [C]", "Gen. winding [W] temp. - AVE [C]", "Gen. winding [W] temp. - MAX [C]", "Generator choke temp. - AVE [C]", "Generator choke temp. - MAX [C]", "Hub cab. 1 temp. - AVE [C]", "Hub cab. 2 temp. - AVE [C]", "Hub cab. 3 temp. - AVE [C]", "Line choke temp. - AVE [C]", "Line choke temp. - MAX [C]", "LSC IGBT Temp[AVE]", "LSC IGBT Temp[MAX]","Nacelle cab. 1 temp. - AVE [C]", "Nacelle cab. 1 temp. - MAX [C]", "Temperature inside converter cabinet 2 - AVE [C]","Temperature inside converter cabinet 2 - MAX [C]", "Tower temp. - AVE [C]", "Tower temp. - MAX [C]", "Towerbase cab. 1 temp. - AVE [C]", "Towerbase cab. 1 temp. - MAX [C]","Wind speed - AVE [m/s]", "Wind speed - MAX [m/s]"]

        # Convert specified columns to numeric, coercing errors to NaN
        final_df[columns_to_modify] = final_df[columns_to_modify].apply(pd.to_numeric, errors='coerce')

        # Replace values <= 0 with NaN in the specified columns
        final_df[columns_to_modify] = final_df[columns_to_modify].applymap(lambda x: np.nan if x <= 0 else x)

        avg_list = ["Blade 1 converter internal temperature - AVE [C]","Blade 2 converter internal temperature - AVE [C]", "Blade 3 converter internal temperature - AVE [C]",
        "Converter cab. 1 temp. - AVE [C]", "Cooling plate temp. - AVE [C]", "Gearbox oil heater temp. - AVE [C]",
        "Gearbox oil tank temp. - AVE [C]", "Gearbox rotor bearing temp. - AVE [C]",
        "Gearbox shaft bearing temp. 1 - AVE [C]", "Gearbox shaft bearing temp. 2 - AVE [C]",
        "Gearbox shaft bearing temp. 3 - AVE [C]", "GSC IGBT Temp[AVE]", "Gen. bearing drive temp. - AVE [C]",
        "Gen. bearing non-drive temp. - AVE [C]", "Gen. water inlet temp. - AVE [C]", "Gen. winding [U] temp. - AVE [C]",
        "Gen. winding [V] temp. - AVE [C]", "Gen. winding [W] temp. - AVE [C]", "Generator choke temp. - AVE [C]",
        "Hub cab. 1 temp. - AVE [C]", "Hub cab. 2 temp. - AVE [C]", "Hub cab. 3 temp. - AVE [C]",
        "Line choke temp. - AVE [C]", "LSC IGBT Temp[AVE]", "Nacelle cab. 1 temp. - AVE [C]",
        "Temperature inside converter cabinet 2 - AVE [C]", "Tower temp. - AVE [C]", "Towerbase cab. 1 temp. - AVE [C]",
        "Wind speed - AVE [m/s]"]
    
        max_list = ["Converter cab. 1 temp. - MAX [C]", 
        "Cooling plate temp. - MAX [C]", "Gearbox oil heater temp. - MAX [C]", 
        "Gearbox oil tank temp. - MAX [C]", 
        "Gearbox rotor bearing temp. - MAX [C]", "Gearbox shaft bearing temp. 1 - MAX [C]", 
        "Gearbox shaft bearing temp. 2 - MAX [C]", "Gearbox shaft bearing temp. 3 - MAX [C]", 
        "GSC IGBT Temp[MAX]", "Gen. bearing drive temp. - MAX [C]", 
        "Gen. bearing non-drive temp. - MAX [C]", "Gen. water inlet temp. - MAX [C]", 
        "Gen. winding [U] temp. - MAX [C]", "Gen. winding [V] temp. - MAX [C]", 
        "Gen. winding [W] temp. - MAX [C]", "Generator choke temp. - MAX [C]", 
        "Line choke temp. - MAX [C]", "LSC IGBT Temp[MAX]", "Nacelle cab. 1 temp. - MAX [C]", 
        "Temperature inside converter cabinet 2 - MAX [C]", 
        "Tower temp. - MAX [C]", "Towerbase cab. 1 temp. - MAX [C]", 
        "Wind speed - MAX [m/s]"]

        # List of index columns for the pivot table
        index_columns = ["Site", "Turbine", "Week"]


        sum_list = ["Active power - AVE [kW]"]

        # Combine both lists and specify aggregation functions
        agg_dict = {col: 'mean' for col in avg_list}
        agg_dict.update({col: 'sum' for col in sum_list})


        # Pivot table with specified aggregation functions
        pivot_df_avg = final_df.pivot_table(values=avg_list + sum_list, index=index_columns, aggfunc=agg_dict)
        pivot_df_avg = pd.DataFrame(pivot_df_avg)


        # Pivot table with maximum values
        pivot_df_max = final_df.pivot_table(values=max_list, index=index_columns, aggfunc='max')

        # Reset index to flatten the pivot tables
        pivot_df_avg.reset_index(inplace=True)
        pivot_df_max.reset_index(inplace=True)

        final_new_df = pd.concat([pivot_df_avg, pivot_df_max], ignore_index=True)   

        pivot_df_avg = pd.DataFrame(pivot_df_avg)
        pivot_df_max = pd.DataFrame(pivot_df_max)

        
        # Merging the dataframes on the specified columns
        merged_df = pd.merge(
            pivot_df_avg,
            pivot_df_max,
            on=["Turbine", "Site", "Week"]
        )

        # Iterate through the rows of df_filtered
        for index, row in merged_df.iterrows():
            turbine = row['Turbine' ]
            # Iterate through the columns of site_name DataFrame
            for col in customer_name.columns:
                # Check if the turbine exists in the current column of site_name
                if turbine in customer_name[ col ].values:
                    merged_df.at[ index, 'Customer' ] = str(col)  # Add the index name of the column to 'Site' column
                    break  # Break the loop if the turbine is found


        merged_df = merged_df[ [ "Site","Turbine", "Week", "Customer", "Active power - AVE [kW]","Wind speed - AVE [m/s]", "Wind speed - MAX [m/s]", "Converter cab. 1 temp. - AVE [C]", "Converter cab. 1 temp. - MAX [C]", "Cooling plate temp. - AVE [C]", "Cooling plate temp. - MAX [C]", "GSC IGBT Temp[AVE]", "GSC IGBT Temp[MAX]", "LSC IGBT Temp[AVE]", "LSC IGBT Temp[MAX]", "Gen. water inlet temp. - AVE [C]", "Gen. water inlet temp. - MAX [C]", "Generator choke temp. - AVE [C]", "Generator choke temp. - MAX [C]", "Line choke temp. - AVE [C]", "Line choke temp. - MAX [C]", "Gearbox rotor bearing temp. - AVE [C]", "Gearbox rotor bearing temp. - MAX [C]", "Gearbox shaft bearing temp. 1 - AVE [C]", "Gearbox shaft bearing temp. 1 - MAX [C]", "Gearbox shaft bearing temp. 2 - AVE [C]", "Gearbox shaft bearing temp. 2 - MAX [C]", "Gearbox shaft bearing temp. 3 - AVE [C]", "Gearbox shaft bearing temp. 3 - MAX [C]", "Gen. bearing drive temp. - AVE [C]", "Gen. bearing drive temp. - MAX [C]", "Gen. bearing non-drive temp. - AVE [C]", "Gen. bearing non-drive temp. - MAX [C]",   "Gearbox oil tank temp. - AVE [C]", "Gearbox oil tank temp. - MAX [C]", "Blade 1 converter internal temperature - AVE [C]", "Blade 2 converter internal temperature - AVE [C]", "Blade 3 converter internal temperature - AVE [C]", "Gen. winding [U] temp. - AVE [C]", "Gen. winding [U] temp. - MAX [C]", "Gen. winding [V] temp. - AVE [C]", "Gen. winding [V] temp. - MAX [C]", "Gen. winding [W] temp. - AVE [C]", "Gen. winding [W] temp. - MAX [C]", "Hub cab. 1 temp. - AVE [C]", "Hub cab. 2 temp. - AVE [C]", "Hub cab. 3 temp. - AVE [C]", "Nacelle cab. 1 temp. - AVE [C]", "Nacelle cab. 1 temp. - MAX [C]", "Temperature inside converter cabinet 2 - AVE [C]", "Temperature inside converter cabinet 2 - MAX [C]", "Tower temp. - AVE [C]", "Tower temp. - MAX [C]", "Towerbase cab. 1 temp. - AVE [C]", "Towerbase cab. 1 temp. - MAX [C]" ] ]
        
        # List of site names
        site_list = ['Kalorana', 'Khanapur', 'Mahidad', 'Tadipatri', 'Dangri', 'Nipaniya', 'Savarkundla', 'Rojmal', 'Bhendewade','Jath','CkPalli']
        
        # Assuming 'df' is your existing DataFrameŚ
        # Create a dictionary for the new rows based on the provided data

        error_set_point = {"Site":"Stop","Blade 1 converter internal temperature - AVE [C]": 60, "Blade 2 converter internal temperature - AVE [C]": 60, "Blade 3 converter internal temperature - AVE [C]": 60, "Converter cab. 1 temp. - AVE [C]": 60, "Converter cab. 1 temp. - MAX [C]": 60, "Cooling plate temp. - AVE [C]": 60, "Cooling plate temp. - MAX [C]": 60, "Gearbox oil tank temp. - AVE [C]": 80, "Gearbox oil tank temp. - MAX [C]": 90, "Gearbox rotor bearing temp. - AVE [C]": 90, "Gearbox rotor bearing temp. - MAX [C]": 90, "Gearbox shaft bearing temp. 1 - AVE [C]": 90, "Gearbox shaft bearing temp. 1 - MAX [C]": 90, "Gearbox shaft bearing temp. 2 - AVE [C]": 90, "Gearbox shaft bearing temp. 2 - MAX [C]": 90, "Gearbox shaft bearing temp. 3 - AVE [C]": 90, "Gearbox shaft bearing temp. 3 - MAX [C]": 90, "GSC IGBT Temp[AVE]": 90, "GSC IGBT Temp[MAX]": 90, "Gen. bearing drive temp. - AVE [C]": 95, "Gen. bearing drive temp. - MAX [C]": 95, "Gen. bearing non-drive temp. - AVE [C]": 95, "Gen. bearing non-drive temp. - MAX [C]": 95, "Gen. water inlet temp. - AVE [C]": 60, "Gen. water inlet temp. - MAX [C]": 60, "Gen. winding [U] temp. - AVE [C]": 145, "Gen. winding [U] temp. - MAX [C]": 145, "Gen. winding [V] temp. - AVE [C]": 145, "Gen. winding [V] temp. - MAX [C]": 145, "Gen. winding [W] temp. - AVE [C]": 145, "Gen. winding [W] temp. - MAX [C]": 145, "Generator choke temp. - AVE [C]": 145, "Generator choke temp. - MAX [C]": 145, "Hub cab. 1 temp. - AVE [C]": 70, "Hub cab. 2 temp. - AVE [C]": 70, "Hub cab. 3 temp. - AVE [C]": 70, "Line choke temp. - AVE [C]": 145, "Line choke temp. - MAX [C]": 145, "LSC IGBT Temp[AVE]": 90, "LSC IGBT Temp[MAX]": 90, "Nacelle cab. 1 temp. - AVE [C]": 60, "Nacelle cab. 1 temp. - MAX [C]": 60,  "Temperature inside converter cabinet 2 - AVE [C]": 61, "Temperature inside converter cabinet 2 - MAX [C]": 61, "Tower temp. - AVE [C]": 60, "Tower temp. - MAX [C]": 60, "Towerbase cab. 1 temp. - AVE [C]": 60, "Towerbase cab. 1 temp. - MAX [C]": 60}

        warning_set_point = {"Site":"Warning","Blade 1 converter internal temperature - AVE [C]": 55, "Blade 2 converter internal temperature - AVE [C]": 55, "Blade 3 converter internal temperature - AVE [C]": 55, "Converter cab. 1 temp. - AVE [C]": 55, "Converter cab. 1 temp. - MAX [C]": 55, "Cooling plate temp. - AVE [C]": 55, "Cooling plate temp. - MAX [C]": 55, "Gearbox oil tank temp. - AVE [C]": 75, "Gearbox oil tank temp. - MAX [C]": 85, "Gearbox rotor bearing temp. - AVE [C]": 85, "Gearbox rotor bearing temp. - MAX [C]": 85, "Gearbox shaft bearing temp. 1 - AVE [C]": 85, "Gearbox shaft bearing temp. 1 - MAX [C]": 85, "Gearbox shaft bearing temp. 2 - AVE [C]": 85, "Gearbox shaft bearing temp. 2 - MAX [C]": 85, "Gearbox shaft bearing temp. 3 - AVE [C]": 85, "Gearbox shaft bearing temp. 3 - MAX [C]": 85, "GSC IGBT Temp[AVE]": 85, "GSC IGBT Temp[MAX]": 85, "Gen. bearing drive temp. - AVE [C]": 90, "Gen. bearing drive temp. - MAX [C]": 90, "Gen. bearing non-drive temp. - AVE [C]": 90, "Gen. bearing non-drive temp. - MAX [C]": 90, "Gen. water inlet temp. - AVE [C]": 50, "Gen. water inlet temp. - MAX [C]": 50, "Gen. winding [U] temp. - AVE [C]": 135, "Gen. winding [U] temp. - MAX [C]": 135, "Gen. winding [V] temp. - AVE [C]": 135, "Gen. winding [V] temp. - MAX [C]": 135, "Gen. winding [W] temp. - AVE [C]": 135, "Gen. winding [W] temp. - MAX [C]": 135, "Generator choke temp. - AVE [C]": 140, "Generator choke temp. - MAX [C]": 140, "Hub cab. 1 temp. - AVE [C]": 60, "Hub cab. 2 temp. - AVE [C]": 60, "Hub cab. 3 temp. - AVE [C]": 60, "Line choke temp. - AVE [C]": 140, "Line choke temp. - MAX [C]": 140, "LSC IGBT Temp[AVE]": 85, "LSC IGBT Temp[MAX]": 85, "Nacelle cab. 1 temp. - AVE [C]": 55, "Nacelle cab. 1 temp. - MAX [C]": 55, "Temperature inside converter cabinet 2 - AVE [C]": 58, "Temperature inside converter cabinet 2 - MAX [C]": 58, "Tower temp. - AVE [C]": 55, "Tower temp. - MAX [C]": 55, "Towerbase cab. 1 temp. - AVE [C]": 55, "Towerbase cab. 1 temp. - MAX [C]": 55}
        
        warning_set_point_below_point = {"Site":"Moderate", "Converter cab. 1 temp. - AVE [C]": 48, "Converter cab. 1 temp. - MAX [C]": 48, "Cooling plate temp. - AVE [C]": 48, "Cooling plate temp. - MAX [C]": 48, "GSC IGBT Temp[AVE]": 72, "GSC IGBT Temp[MAX]": 72, "LSC IGBT Temp[AVE]": 72, "LSC IGBT Temp[MAX]": 72, "Gen. water inlet temp. - AVE [C]": 48, "Gen. water inlet temp. - MAX [C]": 48, "Generator choke temp. - AVE [C]": 116, "Generator choke temp. - MAX [C]": 116, "Line choke temp. - AVE [C]": 116, "Line choke temp. - MAX [C]": 116, "Gearbox rotor bearing temp. - AVE [C]": 72, "Gearbox rotor bearing temp. - MAX [C]": 72, "Gearbox shaft bearing temp. 1 - AVE [C]": 72, "Gearbox shaft bearing temp. 1 - MAX [C]": 72, "Gearbox shaft bearing temp. 2 - AVE [C]": 72, "Gearbox shaft bearing temp. 2 - MAX [C]": 72, "Gearbox shaft bearing temp. 3 - AVE [C]": 72, "Gearbox shaft bearing temp. 3 - MAX [C]": 72, "Gen. bearing drive temp. - AVE [C]": 76, "Gen. bearing drive temp. - MAX [C]": 76, "Gen. bearing non-drive temp. - AVE [C]": 76, "Gen. bearing non-drive temp. - MAX [C]": 76, "Gearbox oil tank temp. - AVE [C]": 64, "Gearbox oil tank temp. - MAX [C]": 72, "Blade 1 converter internal temperature - AVE [C]": 48, "Blade 2 converter internal temperature - AVE [C]": 48, "Blade 3 converter internal temperature - AVE [C]": 48, "Gen. winding [U] temp. - AVE [C]": 116, "Gen. winding [U] temp. - MAX [C]": 116, "Gen. winding [V] temp. - AVE [C]": 116, "Gen. winding [V] temp. - MAX [C]": 116, "Gen. winding [W] temp. - AVE [C]": 116, "Gen. winding [W] temp. - MAX [C]": 116, "Hub cab. 1 temp. - AVE [C]": 56, "Hub cab. 2 temp. - AVE [C]": 56, "Hub cab. 3 temp. - AVE [C]": 56, "Nacelle cab. 1 temp. - AVE [C]": 48, "Nacelle cab. 1 temp. - MAX [C]": 48, "Temperature inside converter cabinet 2 - AVE [C]": 49, "Temperature inside converter cabinet 2 - MAX [C]": 49, "Tower temp. - AVE [C]": 40, "Tower temp. - MAX [C]": 40, "Towerbase cab. 1 temp. - AVE [C]": 48, "Towerbase cab. 1 temp. - MAX [C]": 48}

        # Convert the dictionaries to DataFrames
        df_error_set_point = pd.DataFrame([error_set_point])
        df_warning_set_point = pd.DataFrame([warning_set_point])
        df_warning_set_point_below_point = pd.DataFrame([warning_set_point_below_point])

        # Define threshold and warning colors
        red_fill = 'background-color: red'
        orange_fill = 'background-color: orange'
        green_fill = 'background-color: lightgreen'

        df_Kalorana = pd.DataFrame()
        df_Kalorana = merged_df[merged_df['Site'] == 'Kalorana']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Kalorana = pd.concat([df_Kalorana.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Kalorana.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Kalorana.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Kalorana.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Kalorana.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Kalorana_df = df_Kalorana.style.apply(apply_conditional_formatting)

        df_Khanapur = pd.DataFrame()
        df_Khanapur = merged_df[merged_df['Site'] == 'Khanapur']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Khanapur = pd.concat([df_Khanapur.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Khanapur.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Khanapur.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Khanapur.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Khanapur.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Khanapur_df = df_Khanapur.style.apply(apply_conditional_formatting)

        df_Mahidad = pd.DataFrame()
        df_Mahidad = merged_df[merged_df['Site'] == 'Mahidad']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Mahidad = pd.concat([df_Mahidad.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Mahidad.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Mahidad.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Mahidad.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Mahidad.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Mahidad_df = df_Mahidad.style.apply(apply_conditional_formatting)

        df_Tadipatri = pd.DataFrame()
        df_Tadipatri = merged_df[merged_df['Site'] == 'Tadipatri']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Tadipatri = pd.concat([df_Tadipatri.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Tadipatri.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Tadipatri.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Tadipatri.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Tadipatri.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        # Apply conditional formatting using the style property
        Tadipatri_df = df_Tadipatri.style.apply(apply_conditional_formatting)

        df_Bhendewade = pd.DataFrame()
        df_Bhendewade = merged_df[merged_df['Site'] == 'Bhendewade']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Bhendewade = pd.concat([df_Bhendewade.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Bhendewade.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Bhendewade.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Bhendewade.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Bhendewade.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Bhendewade_df = df_Bhendewade.style.apply(apply_conditional_formatting)

        df_Dangri = pd.DataFrame()
        df_Dangri = merged_df[merged_df['Site'] == 'Dangri']
        
        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Dangri = pd.concat([df_Dangri.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Dangri.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Dangri.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Dangri.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Dangri.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        

        # Apply conditional formatting using the style property
        Dangri_df = df_Dangri.style.apply(apply_conditional_formatting)

        df_Nipaniya = pd.DataFrame()
        df_Nipaniya = merged_df[merged_df['Site'] == 'Nipaniya']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Nipaniya = pd.concat([df_Nipaniya.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Nipaniya.iloc[0:]]).reset_index(drop=True)


        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Nipaniya.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Nipaniya.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Nipaniya.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)

        # Apply conditional formatting using the style property
        Nipaniya_df = df_Nipaniya.style.apply(apply_conditional_formatting)


        df_Savarkundla = pd.DataFrame()
        df_Savarkundla = merged_df[merged_df['Site'] == 'Savarkundla']
        
        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Savarkundla = pd.concat([df_Savarkundla.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Savarkundla.iloc[0:]]).reset_index(drop=True)
        
        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Savarkundla.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Savarkundla.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Savarkundla.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Savarkundla_df = df_Savarkundla.style.apply(apply_conditional_formatting)


        df_Rojmal = pd.DataFrame()
        df_Rojmal = merged_df[merged_df['Site'] == 'Rojmal']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Rojmal = pd.concat([df_Rojmal.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Rojmal.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Rojmal.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Rojmal.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Rojmal.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Rojmal_df = df_Rojmal.style.apply(apply_conditional_formatting)


        df_Jath = pd.DataFrame()
        df_Jath = merged_df[merged_df['Site'] == 'Jath']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_Jath = pd.concat([df_Jath.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_Jath.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_Jath.iloc[1][col.name]  # Get threshold value for the column
            warning = df_Jath.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_Jath.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        Jath_df = df_Jath.style.apply(apply_conditional_formatting)


        df_CkPalli = pd.DataFrame()
        df_CkPalli = merged_df[merged_df['Site'] == 'CkPalli']

        # Insert these DataFrames into the existing DataFrame at the 2nd and 3rd positions
        df_CkPalli = pd.concat([df_CkPalli.iloc[:0], df_error_set_point, df_warning_set_point,df_warning_set_point_below_point, df_CkPalli.iloc[0:]]).reset_index(drop=True)

        # Define a function to apply conditional formatting
        def apply_conditional_formatting(col):
            threshold = df_CkPalli.iloc[1][col.name]  # Get threshold value for the column
            warning = df_CkPalli.iloc[2][col.name]    # Get warning value for the column
            
            def color_cell(value):
                if pd.notna(value):
                    try:
                        value = float(value)  # Convert value to float (if it's not already)
                    except ValueError:
                        return ''  # Return empty string if value cannot be converted to float
                    
                    if col.name in df_CkPalli.columns[:7]:  # Check if the column is within the first 7 columns
                        return ''  # Return empty string if it's within the first 7 columns
                    
                    if value >= threshold:
                        return red_fill
                    elif value >= warning:
                        return orange_fill
                    else:
                        return green_fill
                return ''  # Return empty string for NaN values
            
            return col.apply(color_cell)
        
        # Apply conditional formatting using the style property
        CkPalli_df = df_CkPalli.style.apply(apply_conditional_formatting)


        output_excel_file = os.path.join(folder_path, 'final_output12.xlsx')
        with pd.ExcelWriter(output_excel_file) as writer:
            final_df.to_excel(writer, sheet_name='final_df', index=False)
            pivot_df_avg.to_excel(writer, sheet_name='Average', index=False)
            pivot_df_max.to_excel(writer, sheet_name='Maximum', index=False)
            merged_df.to_excel(writer, sheet_name='combine', index=False)
            Kalorana_df.to_excel(writer, sheet_name='Kalorana', index=False)
            Khanapur_df.to_excel(writer, sheet_name='Khanapur', index=False)
            Mahidad_df.to_excel(writer, sheet_name='Mahidad', index=False)
            Tadipatri_df.to_excel(writer, sheet_name='Tadipatri', index=False)
            Bhendewade_df.to_excel(writer, sheet_name='Bhendewade', index=False)
            Dangri_df.to_excel(writer, sheet_name='Dangri', index=False)
            Nipaniya_df.to_excel(writer, sheet_name='Nipaniya', index=False)
            Savarkundla_df.to_excel(writer, sheet_name='Savarkundla', index=False)
            Rojmal_df.to_excel(writer, sheet_name='Rojmal', index=False) 
            Jath_df.to_excel(writer, sheet_name='Jath', index=False)  
            CkPalli_df.to_excel(writer, sheet_name='CkPalli', index=False)         
        print(f'Final output written to {output_excel_file}')
        



    # Replace 'your_folder_path' with the actual path of the folder containing the zip files
    # your_folder_path = r"Z:\INOX\Inox Temp\Test"
    unzip_all_files_in_folder(folder_path)




    # # Assuming 'df' is your existing DataFrame
    # # Create a dictionary for the new rows based on the provided data

    # error_set_point = {
    #     "Parameters": "Error Set Point",
    #     "Blade 1 converter internal temperature - AVE [C]": 60, "Blade 2 converter internal temperature - AVE [C]": 60, "Blade 3 converter internal temperature - AVE [C]": 60, "Converter cab. 1 temp. - AVE [C]": None, "Converter cab. 1 temp. - MAX [C]": 60, "Cooling plate temp. - AVE [C]": 60, "Cooling plate temp. - MAX [C]": 80, "Gearbox oil tank temp. - AVE [C]": 80, "Gearbox oil tank temp. - MAX [C]": 90, "Gearbox rotor bearing temp. - AVE [C]": 90, "Gearbox rotor bearing temp. - MAX [C]": 90, "Gearbox shaft bearing temp. 1 - AVE [C]": 90, "Gearbox shaft bearing temp. 1 - MAX [C]": 90, "Gearbox shaft bearing temp. 2 - AVE [C]": 90, "Gearbox shaft bearing temp. 2 - MAX [C]": 90, "Gearbox shaft bearing temp. 3 - AVE [C]": 90, "Gearbox shaft bearing temp. 3 - MAX [C]": 90, "GSC IGBT Temp[AVE]": 90, "GSC IGBT Temp[MAX]": 90, "Gen. bearing drive temp. - AVE [C]": 95, "Gen. bearing drive temp. - MAX [C]": 95, "Gen. bearing non-drive temp. - AVE [C]": 95, "Gen. bearing non-drive temp. - MAX [C]": 95, "Gen. water inlet temp. - AVE [C]": 60, "Gen. water inlet temp. - MAX [C]": 60, "Gen. winding [U] temp. - AVE [C]": 145, "Gen. winding [U] temp. - MAX [C]": 145, "Gen. winding [V] temp. - AVE [C]": 145, "Gen. winding [V] temp. - MAX [C]": 145, "Gen. winding [W] temp. - AVE [C]": 145, "Gen. winding [W] temp. - MAX [C]": 145, "Generator choke temp. - AVE [C]": 145, "Generator choke temp. - MAX [C]": 145, "Hub cab. 1 temp. - AVE [C]": None, "Hub cab. 2 temp. - AVE [C]": None, "Hub cab. 3 temp. - AVE [C]": 145, "Line choke temp. - AVE [C]": 145, "Line choke temp. - MAX [C]": 145, "LSC IGBT Temp[AVE]": 90, "LSC IGBT Temp[MAX]": 90, "Nacelle cab. 1 temp. - AVE [C]": None, "Nacelle cab. 1 temp. - MAX [C]": 145, : None,  145, "Temperature inside converter cabinet 2 - AVE [C]": 90, "Temperature inside converter cabinet 2 - MAX [C]": 90, "Tower temp. - AVE [C]": 50, "Tower temp. - MAX [C]": 50, "Towerbase cab. 1 temp. - AVE [C]": None, "Towerbase cab. 1 temp. - MAX [C]": 50}

    # warning_set_point = {
    #     "Parameters": "Warning Set Point", "Blade 1 converter internal temperature - AVE [C]": 55, "Blade 2 converter internal temperature - AVE [C]": 55, "Blade 3 converter internal temperature - AVE [C]": 55, "Converter cab. 1 temp. - AVE [C]": None, "Converter cab. 1 temp. - MAX [C]": 55, "Cooling plate temp. - AVE [C]": 55, "Cooling plate temp. - MAX [C]": 75, "Gearbox oil tank temp. - AVE [C]": 75, "Gearbox oil tank temp. - MAX [C]": 85, "Gearbox rotor bearing temp. - AVE [C]": 85, "Gearbox rotor bearing temp. - MAX [C]": 85, "Gearbox shaft bearing temp. 1 - AVE [C]": 85, "Gearbox shaft bearing temp. 1 - MAX [C]": 85, "Gearbox shaft bearing temp. 2 - AVE [C]": 85, "Gearbox shaft bearing temp. 2 - MAX [C]": 85, "Gearbox shaft bearing temp. 3 - AVE [C]": 85, "Gearbox shaft bearing temp. 3 - MAX [C]": 85, "GSC IGBT Temp[AVE]": 85, "GSC IGBT Temp[MAX]": 85, "Gen. bearing drive temp. - AVE [C]": 90, "Gen. bearing drive temp. - MAX [C]": 90, "Gen. bearing non-drive temp. - AVE [C]": 90, "Gen. bearing non-drive temp. - MAX [C]": 90, "Gen. water inlet temp. - AVE [C]": 50, "Gen. water inlet temp. - MAX [C]": 50, "Gen. winding [U] temp. - AVE [C]": 135, "Gen. winding [U] temp. - MAX [C]": 135, "Gen. winding [V] temp. - AVE [C]": 135, "Gen. winding [V] temp. - MAX [C]": 135, "Gen. winding [W] temp. - AVE [C]": 135, "Gen. winding [W] temp. - MAX [C]": 135, "Generator choke temp. - AVE [C]": 135, "Generator choke temp. - MAX [C]": 135, "Hub cab. 1 temp. - AVE [C]": None, "Hub cab. 2 temp. - AVE [C]": None, "Hub cab. 3 temp. - AVE [C]": 140, "Line choke temp. - AVE [C]": 140, "Line choke temp. - MAX [C]": 140, "LSC IGBT Temp[AVE]": 85, "LSC IGBT Temp[MAX]": 85, "Nacelle cab. 1 temp. - AVE [C]": None, "Nacelle cab. 1 temp. - MAX [C]": 140, : None,  140, "Temperature inside converter cabinet 2 - AVE [C]": 85, "Temperature inside converter cabinet 2 - MAX [C]": 85, "Tower temp. - AVE [C]": 55, "Tower temp. - MAX [C]": 55, "Towerbase cab. 1 temp. - AVE [C]": None, "Towerbase cab. 1 temp. - MAX [C]": 55}

    # # Convert the dictionaries to DataFrames
    # df_error_set_point = pd.DataFrame([error_set_point])
    # df_warning_set_point = pd.DataFrame([warning_set_point])

    # # Append these DataFrames to the existing DataFrame
    # df = df.append(df_error_set_point, ignore_index=True)
    # df = df.append(df_warning_set_point, ignore_index=True)

def resca_tml() :
    return



Daily_Report = {
    "Inox Error Report": inox_error,
    "Inox Warning Report": inox_warning,
    "Resca Error Report": resca_error,
    "Gamesa Error and Warning Report": gamesa_error
}

TML_Report = {"Inox TML": inox_tml, "Resca TML": resca_tml}

Weekly_Report = {"Inox Weekly": inox_tml, "Resca Weekly": resca_tml}

report_types = {"Daily Report": Daily_Report, "TML Report": TML_Report, "Weekly Report": Weekly_Report}

# Global variables to store the single date, start date, end date, and folder link
stored_date = None  # For single date (Daily Report)
stored_start_date = None  # For the start date (TML/Weekly Report)
stored_end_date = None  # For the end date (TML/Weekly Report)
stored_folder_link = None  # For folder link

# Function to store a single date and folder link (for Daily Report)
def store_data(selected_date_str, folder_link):
    global stored_date, stored_folder_link
    stored_date = selected_date_str
    stored_folder_link = folder_link

# Function to store the start and end dates and folder link (for TML/Weekly Report)
def store_date_range(start_date_str, end_date_str, folder_link):
    global stored_start_date, stored_end_date, stored_folder_link
    stored_start_date = start_date_str
    stored_end_date = end_date_str
    stored_folder_link = folder_link

# Function to retrieve the stored single date (Daily Report)
def get_date():
    return stored_date

# Function to retrieve the stored start and end dates (TML/Weekly Report)
def get_date_range():
    return stored_start_date, stored_end_date

# Function to retrieve the stored folder link
def get_folder_link():
    return stored_folder_link

# Title of the application
st.title("SCADA Data Upload and Display")

# Dropdown to select between report types (Daily, TML, Weekly)
selected_report_type = st.selectbox("Select Report Type", list(report_types.keys()), key="report_type_selectbox_unique")

# Depending on the report type selected, present the appropriate options
selected_option = st.selectbox(f"Select an option for {selected_report_type}", list(report_types[selected_report_type].keys()), key="option_selectbox_unique")

# Get the function to execute based on the selection
selected_function = report_types[selected_report_type][selected_option]

# Date input widgets: single date for Daily Report, date range for TML and Weekly Report
if selected_report_type == "Daily Report":
    selected_date = st.date_input("Select a date", value=date.today(), min_value=date(2020, 1, 1), max_value=date.today(), key="date_input_unique")
    if selected_date:
        selected_date_str = selected_date.strftime('%Y-%m-%d')
    else:
        st.error("Please select a valid date.")
        selected_date_str = None
else:
    start_date = st.date_input("Start date", value=date.today(), min_value=date(2020, 1, 1), max_value=date.today(), key="start_date_input_unique")
    end_date = st.date_input("End date", value=date.today(), min_value=date(2020, 1, 1), max_value=date.today(), key="end_date_input_unique")
    if start_date and end_date:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
    else:
        st.error("Please select both a start and an end date.")
        start_date_str = end_date_str = None

# Input for the internal folder link with a unique key
folder_link = st.text_input(f"Enter the folder link for {selected_option}", key="folder_input_unique")

# Button to accept the link and save the date and folder link with a unique key
if st.button("Accept Link", key="accept_button_unique"):
    if folder_link and ((selected_report_type == "Daily Report" and selected_date_str) or (selected_report_type != "Daily Report" and start_date_str and end_date_str)):
        try:
            # List all supported files in the folder
            files = [f for f in os.listdir(folder_link) if f.endswith(('.xlsx', '.xls', '.csv', '.zip'))]
            
            if files:
                # Initialize counters
                zip_file_count = 0
                non_zip_file_count = 0
                
                # Iterate through files to count zip files and other supported files
                for file in files:
                    if file.endswith('.zip'):
                        zip_file_count += 1
                    else:
                        non_zip_file_count += 1
                
                # Display the count of files
                st.write(f"Number of .zip files found: {zip_file_count}")
                st.write(f"Number of non-.zip files found: {non_zip_file_count}")
                
                if zip_file_count > 0:
                    st.write("The following zip files are found in the folder:")
                    for file in files:
                        if file.endswith('.zip'):
                            st.write(file)
                
                if non_zip_file_count > 0:
                    # Display the non-zip files found
                    st.write("Non-zip files found in the folder:")
                    selected_file = st.selectbox("Select a file to view", [f for f in files if not f.endswith('.zip')], key="file_selectbox_unique")
                    
                    # Display the selected file's content
                    if selected_file:
                        file_path = os.path.join(folder_link, selected_file)
                        if selected_file.endswith(('.xlsx', '.xls')):
                            df = pd.read_excel(file_path)
                        elif selected_file.endswith('.csv'):
                            df = pd.read_csv(file_path)

                        st.dataframe(df)  # Display the DataFrame
                
                # Store the date and folder link using the appropriate function based on the report type
                if selected_report_type == "Daily Report":
                    store_data(selected_date_str, folder_link)
                    st.write(f"Stored Date: {get_date()}")
                else:
                    store_date_range(start_date_str, end_date_str, folder_link)
                    st.write(f"Stored Start Date: {get_date_range()[0]}")
                    st.write(f"Stored End Date: {get_date_range()[1]}")
                st.write(f"Stored Folder Link: {get_folder_link()}")

                # Process the files only if the folder link is valid
                if callable(selected_function):
                    selected_function()  # Call the function
                else:
                    st.error("Selected module function is not callable.")
            else:
                st.error("No supported files found in the provided folder link.")
        except FileNotFoundError:
            st.error("The specified folder was not found.")
        except PermissionError:
            st.error("Permission denied for the specified folder.")
        except Exception as e:
            st.error(f"Error accessing the folder: {e}")
    else:
        st.error("Please enter a valid folder link and ensure that valid dates are selected.")
