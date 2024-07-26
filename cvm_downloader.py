import requests
from bs4 import BeautifulSoup
import os
import zipfile
from tqdm import tqdm
import pandas as pd
import numpy as np
import unidecode
import time
import multiprocessing as mp
import shutil
from collections import Counter

def download_cvm_zip_files():
    url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/"
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    zip_links = [link.get('href') for link in soup.find_all('a') if link.get('href', '').endswith('.zip')]
    
    if not os.path.exists('cvm_zip_files'):
        os.makedirs('cvm_zip_files')
    
    for link in tqdm(zip_links, desc="Downloading zip files"):
        file_url = url + link
        file_name = os.path.join('cvm_zip_files', link)
        
        if os.path.exists(file_name):
            print(f"File {link} already exists. Skipping download.")
            continue
        
        file_response = requests.get(file_url, stream=True)
        total_size = int(file_response.headers.get('content-length', 0))
        
        with open(file_name, 'wb') as file, tqdm(
            desc=link,
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as progress_bar:
            for data in file_response.iter_content(chunk_size=1024):
                size = file.write(data)
                progress_bar.update(size)
    
    print("All zip files have been downloaded.")

def is_valid_file(filename):
    # Keep files with the format dfp_cia_aberta_{year}.csv
    if filename.startswith('dfp_cia_aberta_') and filename.count('_') == 3:
        return True
    
    if 'ind' in filename.lower():
        return False
    if 'MD' in filename:
        return False
    if filename.startswith('dfp_cia_aberta_parecer_'):
        return False
    parts = filename.split('_')
    if len(parts) >= 5:
        statement = parts[3]
        if statement in ['DVA', 'DRA', 'DMPL', 'DFC_MD']:
            return False
    return True

def unify_csv_files(overwrite=False):
    zip_dir = 'cvm_zip_files'
    output_dir = 'unified_cvm_data'
    temp_dir = 'temp_csv_files'

    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Extract all CSV files from the downloaded zip files
    zip_files = [f for f in os.listdir(zip_dir) if f.endswith('.zip')]
    for zip_file in tqdm(zip_files, desc="Extracting zip files"):
        try:
            with zipfile.ZipFile(os.path.join(zip_dir, zip_file), 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
        except zipfile.BadZipFile:
            print(f"Error: {zip_file} is not a valid zip file. Skipping.")
            continue

    # Move and filter CSV files
    csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
    for file in tqdm(csv_files, desc="Unifying CSV files"):
        if is_valid_file(file):
            src_path = os.path.join(temp_dir, file)
            dst_path = os.path.join(output_dir, file)
            if os.path.exists(dst_path):
                if overwrite:
                    os.remove(dst_path)
                    shutil.move(src_path, dst_path)
                else:
                    print(f"File {file} already exists in the unified folder. Skipping.")
            else:
                shutil.move(src_path, dst_path)

    # Clean up: remove temporary directory and cvm_zip_files
    shutil.rmtree(temp_dir)
    shutil.rmtree(zip_dir)

    print(f"Filtered CSV files have been unified into {output_dir}")
    return output_dir

def count_files_by_year(directory):
    year_count = Counter()
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            year = file.split('_')[-1].split('.')[0]
            year_count[year] += 1
    
    print("\nNumber of files by year:")
    for year, count in sorted(year_count.items()):
        print(f"{year}: {count}")

def read_files(file_type):
    files = os.listdir('unified_cvm_data')
    if file_type == 'BS':
        files = [file for file in files if 'dfp_cia_aberta_BPA_con_' in file or 'dfp_cia_aberta_BPP_con_' in file]
    elif file_type == 'IS':
        files = [file for file in files if 'dfp_cia_aberta_DRE_con_' in file]
    elif file_type == 'CF':
        files = [file for file in files if 'dfp_cia_aberta_DFC_MI_con_' in file]
    else:
        return None

    df = pd.concat([pd.read_csv(f'unified_cvm_data/{file}', sep=';', encoding='latin1', dtype={'VL_CONTA': str})
                    for file in files], ignore_index=True)

    df['VL_CONTA'] = pd.to_numeric(df['VL_CONTA'].str.replace(',', '.'), errors='coerce')
    df['CD_CVM'] = df['CD_CVM'].astype(int)

    columns_to_drop = ['CNPJ_CIA', 'VERSAO', 'DT_INI_EXERC']
    if 'ORDEM_EXERC' in df.columns:
        df = df[df['ORDEM_EXERC'] != 'PENÚLTIMO']
        columns_to_drop.append('ORDEM_EXERC')

    df = df.drop(columns=columns_to_drop, errors='ignore')

    if file_type == 'BS':
        df['GRUPO_DFP'] = df['GRUPO_DFP'].replace({
            'DF Consolidado - Balanço Patrimonial Ativo': 'BSA',
            'DF Consolidado - Balanço Patrimonial Passivo': 'BSP'
        })
    elif file_type == 'IS':
        df['GRUPO_DFP'] = df['GRUPO_DFP'].replace('DF Consolidado - Demonstração do Resultado', 'DRE - Con')
    elif file_type == 'CF':
        df['GRUPO_DFP'] = df['GRUPO_DFP'].replace('DF Consolidado - Demonstração do Fluxo de Caixa (Método Indireto)', 'FC-MI')

    return df

def read_files_ref():
    files = [file for file in os.listdir('unified_cvm_data') if 'dfp_cia_aberta_20' in file]
    df = pd.concat([pd.read_csv(f'unified_cvm_data/{file}', sep=';', encoding='latin1') for file in files], ignore_index=True)
    df = df.drop(columns=['CNPJ_CIA', 'VERSAO', 'ID_DOC', 'DT_RECEB', 'LINK_DOC'], errors='ignore')
    df['CD_CVM'] = df['CD_CVM'].astype(int)
    return df.drop_duplicates(subset='CD_CVM')

def aggregate_df(df):
    return df.groupby(['GRUPO_DFP','CD_CVM','DENOM_CIA','CD_CONTA', 'DS_CONTA','ST_CONTA_FIXA', 'DT_FIM_EXERC'], as_index=False)['VL_CONTA'].sum()

def remove_accents(text): 
     return unidecode.unidecode(text)

def process_statement(statement_type):
    df = read_files(statement_type)
    df = aggregate_df(df)
    df.columns = [remove_accents(col) for col in df.columns]
    return df

def create_csv_files(n=None):
    start_time = time.time()

    ref_df = read_files_ref()
    unique_cd_cvms = ref_df['CD_CVM'].unique()

    if n is not None:
        unique_cd_cvms = unique_cd_cvms[:n]

    print(f"Number of CD_CVMs being processed: {len(unique_cd_cvms)}")

    read_time = time.time()
    print(f"Time to read reference data: {read_time - start_time:.2f} seconds")

    # Use multiprocessing to process statements in parallel
    with mp.Pool(processes=3) as pool:
        results = pool.map(process_statement, ['CF', 'BS', 'IS'])

    process_time = time.time()
    print(f"Time to process statements: {process_time - read_time:.2f} seconds")

    cf_data, bs_data, is_data = results

    # Save to CSV files, overwriting existing files
    cf_data.to_csv('cash_flows.csv', index=False, mode='w', encoding='utf-8', decimal='.')
    bs_data.to_csv('balance_sheets.csv', index=False, mode='w', encoding='utf-8', decimal='.')
    is_data.to_csv('income_statments.csv', index=False, mode='w', encoding='utf-8', decimal='.')

    save_time = time.time()
    print(f"Time to save CSV files: {save_time - process_time:.2f} seconds")

    total_time = time.time() - start_time
    print(f"\nTotal time elapsed: {total_time:.2f} seconds")

    print("CSV files creates succesfully!")

def init_cvm_downloader(overwrite=False):
    print("initiating cvm downloader")
    download_cvm_zip_files()
    print("zip files downloaded")
    print("unifying csv files") 
    unify_csv_files(overwrite)
    print("zip files unified")
    print("creating csv files")
    create_csv_files()
    print("csv files created")

if __name__ == '__main__':
    init_cvm_downloader(overwrite=True)  # Set to False if you don't want to overwrite existing files