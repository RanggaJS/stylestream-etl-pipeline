#!/usr/bin/env python3
"""
Module untuk melakukan ekstraksi data dari website fashion studio.
"""
import logging
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Konfigurasi logging
logger = logging.getLogger(__name__)

# URL Target
BASE_URL = "https://fashion-studio.dicoding.dev"
MAX_RETRIES = 3
RETRY_DELAY = 2

def scrape_page(page_number):
    """
    Melakukan scraping pada satu halaman website.
    
    Args:
        page_number (int): Nomor halaman yang akan di-scrape
        
    Returns:
        list: Daftar produk yang berhasil di-scrape dari halaman tersebut
    
    Raises:
        Exception: Jika terjadi kesalahan saat melakukan request atau parsing
    """
    # URL yang diubah
    if page_number == 1:
        url = BASE_URL
    else:
        url = f"{BASE_URL}/page{page_number}"
    
    products = []
    
    # Implementasi retry untuk mengatasi kendala jaringan
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Mengambil data dari halaman {page_number}...")
            
            # Tambahkan header user-agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Selector untuk produk-produk di halaman
            product_cards = soup.select('.collection-card')
            
            # Debug info
            logger.info(f"Jumlah produk ditemukan: {len(product_cards)}")
            
            # Timestamp sebagai penanda waktu scraping
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for card in product_cards:
                try:
                    # Inisialisasi dictionary untuk menyimpan data produk
                    product = {}
                    
                    # Ekstrak judul produk
                    product_details = card.select_one('.product-details')
                    title_element = card.select_one('.product-title') or (product_details.select_one('h3') if product_details else None)
                    
                    if title_element:
                        product['Title'] = title_element.text.strip()
                    else:
                        # Jika tidak menemukan dengan selector di atas, coba selector lain
                        title_element = card.select_one('h3')
                        if title_element:
                            product['Title'] = title_element.text.strip()
                        else:
                            product['Title'] = "Unknown Product"
                    
                    # Ekstrak harga produk
                    price_container = card.select_one('.price-container')
                    price_element = price_container.select_one('.price') if price_container else None
                    
                    if not price_element:
                        price_element = card.select_one('.price') or card.select_one('span.price')
                    
                    if price_element:
                        price_text = price_element.text.strip()
                        product['Price'] = price_text
                    else:
                        product['Price'] = "Price Unavailable"
                    
                    # Ekstrak rating, colors, size, dan gender dari paragraf
                    # Berdasarkan screenshot, data ini ada di elemen <p> dengan style inline
                    paragraphs = card.select('p')
                    
                    # Default values
                    product['Rating'] = "Invalid Rating"
                    product['Colors'] = "Colors Unavailable"
                    product['Size'] = "Size Unavailable"
                    product['Gender'] = "Gender Unavailable"
                    
                    for p in paragraphs:
                        text = p.text.strip()
                        
                        # Ekstrak rating
                        if "Rating:" in text or ("/" in text and "★" in text):
                            product['Rating'] = text
                        
                        # Ekstrak colors
                        elif "Colors" in text:
                            product['Colors'] = text
                        
                        # Ekstrak size
                        elif text.startswith("Size:") or "Size" in text:
                            product['Size'] = text
                        
                        # Ekstrak gender
                        elif text.startswith("Gender:") or "Gender" in text or "Men" in text or "Women" in text or "Unisex" in text:
                            product['Gender'] = text
                    
                    # Tambahkan timestamp
                    product['timestamp'] = current_timestamp
                    
                    # Debug: cetak produk yang diekstrak
                    logger.info(f"Produk diekstrak: {product}")
                    
                    products.append(product)
                    
                except Exception as e:
                    logger.warning(f"Gagal mengekstrak produk: {str(e)}")
                    continue
                
            logger.info(f"Berhasil mengambil {len(products)} produk dari halaman {page_number}")
            
            # Jika berhasil mendapatkan data, keluar dari loop retry
            if products:
                return products
            
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Percobaan {attempt+1} gagal: {str(e)}. Mencoba kembali dalam {RETRY_DELAY} detik...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Gagal mengambil data setelah {MAX_RETRIES} percobaan: {str(e)}")
                raise Exception(f"Gagal mengakses halaman {page_number}: {str(e)}")
                
    return products

def extract_data(start_page=1, end_page=50):
    """
    Mengekstrak data dari rentang halaman website.
    
    Args:
        start_page (int): Halaman awal untuk ekstraksi
        end_page (int): Halaman akhir untuk ekstraksi
        
    Returns:
        DataFrame: Data hasil ekstraksi dalam format pandas DataFrame
    """
    if start_page < 1:
        logger.warning("Halaman awal minimal adalah 1. Menggunakan halaman awal = 1")
        start_page = 1
        
    if end_page < start_page:
        logger.warning(f"Halaman akhir tidak boleh kurang dari halaman awal. Menggunakan halaman akhir = {start_page}")
        end_page = start_page
    
    all_products = []
    
    try:
        for page in range(start_page, end_page + 1):
            try:
                page_products = scrape_page(page)
                if page_products:
                    all_products.extend(page_products)
                else:
                    logger.warning(f"Tidak ada produk yang diekstrak dari halaman {page}")
                
                # Delay kecil untuk menghindari rate limiting
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Gagal mengambil data dari halaman {page}: {str(e)}")
                continue
        
        # Konversi ke DataFrame
        if all_products:
            df = pd.DataFrame(all_products)
            logger.info(f"Total data yang berhasil diekstrak: {len(df)}")
            
            # Debug: Cetak sampel data
            logger.info(f"Sampel data yang diekstrak:\n{df.head().to_string()}")
            
            # Cetak nilai unik untuk setiap kolom
            for col in df.columns:
                unique_values = df[col].unique()
                if len(unique_values) < 10:  # Hanya cetak jika jumlah nilai unik sedikit
                    logger.info(f"Nilai unik untuk kolom {col}: {unique_values}")
                else:
                    logger.info(f"Jumlah nilai unik untuk kolom {col}: {len(unique_values)}")
            
            return df
        else:
            logger.warning("Tidak ada produk yang berhasil diekstrak")
            # Buat DataFrame kosong dengan kolom yang diperlukan
            return pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp'])
        
    except Exception as e:
        logger.error(f"Terjadi kesalahan pada proses ekstraksi: {str(e)}")
        # Mengembalikan DataFrame kosong daripada gagal sepenuhnya
        return pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'timestamp'])
