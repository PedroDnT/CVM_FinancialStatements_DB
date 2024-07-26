# CVM Financial Statements Database
- Scripts that automatically download and upload consolidated financial statments for companies listed on CVM and save them in a postgres database
- additional scripts to interact with the database and return statments in the usual form 

## cvm_downloader.py
### Steps implemented when running the script cvm_downloader.py
1. Download zip files from CVM website
2. Unzips and unifies file under unified_csv_files folder
3. Creates and saves in the dir CSV for statment balnce_sheet, Income Statement and cash_flow_statement (DFC metodo direto). All from consolidado statements
3.1 The statements are in the form of
   GRUPO_DFP | CD_CVM | DENOM_CIA | CD_CONTA | DS_CONTA | ST_CONTA_FIXA | DT_FIM_EXERC | VL_CONTA
Should run under 3 minutes total maximum. 

## database_uploader.py
### Steps implemented when running the script database_uploader.py
1. Connects do postgres database
2. Creates a table for each statement type
3. Inserts the data from the csv files into the database
4. Commits the transaction

## db_interaction.py
### File contains functions to interact with the database
1. execute_query returns a given staetment pivoted to the usual format of financial statements
2. get_companies_list returns a list of companies from the database
3. get_distinct_cd_cvm returns a list of distinct cd_cvm from the database
4. get_company_name_by_cd_cvm returns the company name given cd_cvm from the database

## Run instructions
1. Create a .env file with the database connection string
2. Run poetry shell (make sure poetry is installed - in MacOS brew install poetry)
2. Run the script cvm_downloader.py
3. Run the script database_uploader.py
4. Use db_interaction.py to interact with the database
