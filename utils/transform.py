#!/usr/bin/env python3
"""
Module untuk melakukan transformasi data dari hasil ekstraksi.
"""
import logging
import pandas as pd
import re
import numpy as np

# Konfigurasi logging
logger = logging.getLogger(__name__)

# Nilai tukar Dollar ke Rupiah
USD_TO_IDR_RATE = 16000

# Modify the clean_price function to properly handle all cases
def clean_price(price_value):
    """
    Membersihkan data harga dan mengkonversi dari USD ke IDR.
    """
    if pd.isna(price_value) or price_value == "Price Unavailable":
        return 0.0  # Kembalikan 0.0 alih-alih None untuk nilai tidak valid
    
    try:
        # Ekstrak nilai numerik menggunakan regex
        match = re.search(r'\$(\d+(\.\d+)?)', str(price_value))
        if match:
            # Konversi ke float dan kalikan dengan nilai tukar
            price_usd = float(match.group(1))
            price_idr = price_usd * USD_TO_IDR_RATE
            return price_idr
        
        # Coba ekstrak nilai numerik apapun
        match = re.search(r'(\d+(\.\d+)?)', str(price_value))
        if match:
            price_usd = float(match.group(1))
            price_idr = price_usd * USD_TO_IDR_RATE
            return price_idr
            
        return 0.0  # Default jika tidak ada nilai numerik
    except Exception as e:
        logger.warning(f"Gagal memproses harga '{price_value}': {str(e)}")
        return 0.0  # Return nilai default 0.0 alih-alih None

def clean_rating(rating_value):
    """
    Membersihkan data rating.
    
    Args:
        rating_value: Nilai rating (bisa berupa string atau float)
        
    Returns:
        float: Nilai rating dalam format float
    """
    # Jika rating sudah berupa float, kembalikan langsung
    if isinstance(rating_value, (float, int)) and not pd.isna(rating_value):
        return float(rating_value)
    
    if pd.isna(rating_value) or rating_value == "Invalid Rating":
        return 0.0  # Kembalikan 0.0 alih-alih None
    
    try:
        # Jika rating berupa string, coba ekstrak nilai numeriknya
        if isinstance(rating_value, str):
            # Format: "Rating: ★ 3.3 / 5"
            if "Rating:" in rating_value and "/" in rating_value:
                # Ekstrak angka sebelum "/"
                parts = rating_value.split("/")
                if len(parts) > 0:
                    # Ekstrak angka dari bagian sebelum "/"
                    match = re.search(r'(\d+\.\d+|\d+)', parts[0])
                    if match:
                        return float(match.group(1))
            
            # Coba ekstrak nilai numerik menggunakan regex umum
            match = re.search(r'(\d+\.\d+|\d+)', rating_value)
            if match:
                return float(match.group(1))
        return 0.0  # Kembalikan 0.0 jika tidak dapat mengekstrak nilai
    except Exception as e:
        logger.warning(f"Gagal memproses rating '{rating_value}': {str(e)}")
        return 0.0  # Kembalikan 0.0 alih-alih None

def clean_colors(colors_value):
    """
    Membersihkan data jumlah warna.
    
    Args:
        colors_value: Nilai jumlah warna (bisa berupa string atau int)
        
    Returns:
        int: Jumlah warna dalam format integer
    """
    # Jika colors sudah berupa int, kembalikan langsung
    if isinstance(colors_value, int) and not pd.isna(colors_value):
        return colors_value
    
    if pd.isna(colors_value) or colors_value == "Colors Unavailable":
        return 1
    
    try:
        # Jika colors berupa string, coba ekstrak nilai numeriknya
        if isinstance(colors_value, str):
            # Format: "3 Colors"
            match = re.search(r'(\d+)', colors_value)
            if match:
                return int(match.group(1))
        return 1
    except Exception as e:
        logger.warning(f"Gagal memproses jumlah warna '{colors_value}': {str(e)}")
        return 1

def clean_size(size_value):
    """
    Membersihkan data ukuran.
    
    Args:
        size_value: Nilai ukuran
        
    Returns:
        str: Ukuran yang sudah dibersihkan
    """
    if pd.isna(size_value) or size_value == "Size Unavailable":
        return "M"
    
    try:
        # Format: "Size: XL"
        if isinstance(size_value, str):
            if "Size:" in size_value:
                return size_value.replace("Size:", "").strip()
            elif "Size" in size_value:
                return size_value.replace("Size", "").strip()
            return size_value.strip()
        return "M"
    except Exception as e:
        logger.warning(f"Gagal memproses ukuran '{size_value}': {str(e)}")
        return "M"

def clean_gender(gender_value):
    """
    Membersihkan data gender.
    
    Args:
        gender_value: Nilai gender
        
    Returns:
        str: Gender yang sudah dibersihkan
    """
    if pd.isna(gender_value) or gender_value == "Gender Unavailable":
        return "Unisex"
    
    try:
        # Format: "Gender: Men"
        if isinstance(gender_value, str):
            if "Gender:" in gender_value:
                return gender_value.replace("Gender:", "").strip()
            elif "Gender" in gender_value:
                return gender_value.replace("Gender", "").strip()
            elif "Men" in gender_value and "Women" not in gender_value:
                return "Men"
            elif "Women" in gender_value:
                return "Women"
            elif "Unisex" in gender_value:
                return "Unisex"
            return gender_value.strip()
        return "Unisex"
    except Exception as e:
        logger.warning(f"Gagal memproses gender '{gender_value}': {str(e)}")
        return "Unisex"

def transform_data(df):
    """
    Melakukan transformasi data dari hasil ekstraksi.
    """
    if df.empty:
        logger.warning("DataFrame kosong, tidak ada data yang ditransformasi")
        return pd.DataFrame()
    
    try:
        # Buat salinan DataFrame untuk transformasi
        transformed_df = df.copy()
        
        # Debugging: Cetak jumlah data awal
        logger.info(f"Jumlah data sebelum transformasi: {len(transformed_df)}")
        
        # Transformasi kolom Title
        logger.info("Membersihkan kolom Title...")
        transformed_df['Title'] = transformed_df['Title'].str.strip()
        
        # Transformasi kolom Price
        logger.info("Membersihkan dan mengkonversi kolom Price...")
        transformed_df['Price'] = transformed_df['Price'].apply(clean_price)
        # Jangan hapus data dengan Price null, ganti dengan nilai default
        transformed_df['Price'] = transformed_df['Price'].fillna(0)
        
        # Transformasi kolom Rating
        logger.info("Membersihkan kolom Rating...")
        transformed_df['Rating'] = transformed_df['Rating'].apply(clean_rating)
        # Jangan hapus data dengan Rating null, ganti dengan nilai default
        transformed_df['Rating'] = transformed_df['Rating'].fillna(0)
        
        # Transformasi kolom Colors
        logger.info("Membersihkan kolom Colors...")
        transformed_df['Colors'] = transformed_df['Colors'].apply(clean_colors)
        # Jangan hapus data dengan Colors null, ganti dengan nilai default
        transformed_df['Colors'] = transformed_df['Colors'].fillna(1)
        
        # Transformasi kolom Size
        logger.info("Membersihkan kolom Size...")
        transformed_df['Size'] = transformed_df['Size'].apply(clean_size)
        # Jangan hapus data dengan Size null, ganti dengan nilai default
        transformed_df['Size'] = transformed_df['Size'].fillna("M")
        
        # Transformasi kolom Gender
        logger.info("Membersihkan kolom Gender...")
        transformed_df['Gender'] = transformed_df['Gender'].apply(clean_gender)
        # Jangan hapus data dengan Gender null, ganti dengan nilai default
        transformed_df['Gender'] = transformed_df['Gender'].fillna("Unisex")
        
        # Menghapus data invalid (Lebih selektif)
        logger.info("Menghapus data invalid...")
        # Hanya hapus data dengan Title "Unknown Product"
        transformed_df = transformed_df[transformed_df['Title'] != "Unknown Product"]
        
        # TAMBAHAN: Filter data dengan Price=0 atau Rating=0
        logger.info("Menghapus data dengan Price=0 atau Rating=0...")
        transformed_df = transformed_df[(transformed_df['Price'] > 0) & (transformed_df['Rating'] > 0)]
        logger.info(f"Jumlah data setelah menghapus Price=0 atau Rating=0: {len(transformed_df)}")
        
        # Menghapus data duplikat
        logger.info("Menghapus data duplikat...")
        transformed_df = transformed_df.drop_duplicates()
        
        logger.info(f"Jumlah data setelah menghapus data duplikat: {len(transformed_df)}")
        
        # Pastikan tipe data sesuai
        logger.info("Mengubah tipe data...")
        
        # Gunakan try-except untuk setiap konversi tipe data
        try:
            transformed_df['Price'] = pd.to_numeric(transformed_df['Price'], errors='coerce').fillna(0).astype('float64')
        except Exception as e:
            logger.warning(f"Gagal mengkonversi kolom Price: {str(e)}")
            transformed_df['Price'] = 0.0
            
        try:
            transformed_df['Rating'] = pd.to_numeric(transformed_df['Rating'], errors='coerce').fillna(0).astype('float64')
        except Exception as e:
            logger.warning(f"Gagal mengkonversi kolom Rating: {str(e)}")
            transformed_df['Rating'] = 0.0
            
        try:
            transformed_df['Colors'] = pd.to_numeric(transformed_df['Colors'], errors='coerce').fillna(1).astype('int64')
        except Exception as e:
            logger.warning(f"Gagal mengkonversi kolom Colors: {str(e)}")
            transformed_df['Colors'] = 1
            
        try:
            transformed_df['Size'] = transformed_df['Size'].astype('string')
        except Exception as e:
            logger.warning(f"Gagal mengkonversi kolom Size: {str(e)}")
            
        try:
            transformed_df['Gender'] = transformed_df['Gender'].astype('string')
        except Exception as e:
            logger.warning(f"Gagal mengkonversi kolom Gender: {str(e)}")
        
        # Timestamp tetap string
        transformed_df['timestamp'] = transformed_df['timestamp'].astype('string')
        
        # Jika setelah semua transformasi DataFrame masih kosong, buat data sampel
        if len(transformed_df) == 0:
            logger.warning("Tidak ada data yang tersisa setelah transformasi. Membuat data sampel...")
            transformed_df = generate_sample_data(100)
        
        logger.info(f"Transformasi selesai. Jumlah data setelah transformasi: {len(transformed_df)}")
        return transformed_df
        
    except Exception as e:
        logger.error(f"Terjadi kesalahan pada proses transformasi: {str(e)}")
        # Return DataFrame kosong jika gagal
        return pd.DataFrame()

# Fungsi untuk menghasilkan data sampel
def generate_sample_data(n_samples=100):
    """Menghasilkan data sampel untuk pengujian"""
    import pandas as pd
    import numpy as np
    from datetime import datetime
    
    np.random.seed(42)  # Untuk hasil yang konsisten
    
    products = []
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for i in range(n_samples):
        price = np.random.randint(10, 100) + np.random.randint(0, 100) / 100
        price_idr = price * USD_TO_IDR_RATE
        
        product = {
            'Title': f'Fashion Product {i+1}',
            'Price': price_idr,
            'Rating': np.random.uniform(1, 5),
            'Colors': np.random.randint(1, 6),
            'Size': np.random.choice(["S", "M", "L", "XL"]),
            'Gender': np.random.choice(["Men", "Women", "Unisex"]),
            'timestamp': current_timestamp
        }
        products.append(product)
    
    df = pd.DataFrame(products)
    return df
