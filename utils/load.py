
import logging
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.exc import SQLAlchemyError
import googleapiclient.discovery
from google.oauth2 import service_account

# Konfigurasi logging
logger = logging.getLogger(__name__)

def save_to_csv(dataframe, filename=None):
   
    if dataframe.empty:
        raise ValueError("DataFrame kosong, tidak ada data yang dapat disimpan")
    
    try:
        if filename is None:
            filename = "products.csv"
        
        # Simpan DataFrame ke CSV
        dataframe.to_csv(filename, index=False)
        
        # Verifikasi file telah dibuat
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File {filename} gagal dibuat")
        
        return os.path.abspath(filename)
        
    except Exception as e:
        logger.error(f"Gagal menyimpan data ke CSV: {str(e)}")
        raise

def save_to_gsheets(dataframe, creds_file="google-sheets-api.json"):
    
    if dataframe.empty:
        raise ValueError("DataFrame kosong, tidak ada data yang dapat disimpan")
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"File kredensial {creds_file} tidak ditemukan")
    
    try:
        # Menyiapkan kredensial
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        
        # Buat instance Google Sheets API dan Drive API
        sheets_service = googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)
        drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)
        
        # Buat spreadsheet baru
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        spreadsheet_body = {
            'properties': {
                'title': f"FashionStudio_Products_{timestamp}"
            }
        }
        
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
        spreadsheet_id = spreadsheet.get('spreadsheetId')
        
        # Ubah permission agar dapat diakses oleh siapa saja dengan link
        permission = {
            'type': 'anyone',
            'role': 'writer',
            'allowFileDiscovery': False
        }
        drive_service.permissions().create(fileId=spreadsheet_id, body=permission).execute()
        
        # Konversi DataFrame ke list values
        values = [dataframe.columns.tolist()]
        values.extend(dataframe.values.tolist())
        
        body = {
            'values': values
        }
        
        # Update data ke sheet
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Sheet1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Dapatkan URL spreadsheet
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        
        return spreadsheet_url
        
    except Exception as e:
        logger.error(f"Gagal menyimpan data ke Google Sheets: {str(e)}")
        raise

def save_to_postgres(dataframe, db_config=None):
    
    if dataframe.empty:
        raise ValueError("DataFrame kosong, tidak ada data yang dapat disimpan")
    
    # Default konfigurasi database
    if db_config is None:
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'port': os.getenv('DB_PORT', '5432')
        }
    
    try:
        # Buat koneksi ke database
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        
        # Simpan DataFrame ke database
        table_name = "fashion_products"
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        
        logger.info(f"Data berhasil disimpan ke tabel {table_name}")
        return True
        
    except SQLAlchemyError as e:
        logger.error(f"Gagal menyimpan data ke PostgreSQL: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Terjadi kesalahan saat menyimpan ke PostgreSQL: {str(e)}")
        raise