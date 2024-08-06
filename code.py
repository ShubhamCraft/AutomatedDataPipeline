import os
import time
import pandas as pd
import pymysql
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import schedule
from EmailNotification import send_email

# Directory to store downloaded files
download_dir = "D:/downloads"
output_directory = "C:/pricelabs/pricelabsdata"
log_file = "C:/Users/bnbme/OneDrive/db_transfer.log"

# Database Credentials
local_db_config = {
    "host": "111.17.11.10",
    "user": "shubham",
    "password": "Bnb@110022#",
    "database": "pricelabs"
}

remote_db_config = {
    "host": "61.171.2311.1412",
    "user": "pluser",
    "password": "122334",
    "database": "pricelabs"
}

# Set up logging
logging.basicConfig(filename=log_file, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def add_plb_area_column(download_dir, selected_option):
    files = os.listdir(download_dir)
    matching_files = [file for file in files if file.startswith("FuturePriceOccupancy")]
    matching_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
    latest_file = matching_files[0]
    latest_file_path = os.path.join(download_dir, latest_file)
    df = pd.read_csv(latest_file_path)
    df["plb_area"] = selected_option
    df.to_csv(latest_file_path, index=False)


def fetch_and_download_reports():
    try:
        driver = webdriver.Chrome()
        driver.get("https://pricelabs.co/portfolio_analytics?filtered_view=1&layout_type=pacing&pms=resharmonics")
        time.sleep(7)

        driver.find_element(By.XPATH, '//*[@id="user_email"]').send_keys("shubhamtwr2@gmail.com")
        driver.find_element(By.XPATH, '//*[@id="user_password"]').send_keys("Streak@123")
        driver.find_element(By.XPATH, '//*[@id="new_user"]/input[3]').click()
        time.sleep(60)

        driver.switch_to.frame(driver.find_element(By.ID, "report-iframe"))
        time.sleep(20)

        market_groups = ["Dubai All", "Dcompetitors"]
        for group in market_groups:
            Mdropdown = WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.XPATH, '//*[@id="market-dashboard"]')))
            Mdropdown.click()
            time.sleep(5)
            for _ in range(2):
                Mdropdown.send_keys(Keys.ARROW_DOWN)
                time.sleep(7)
            Mdropdown.send_keys(Keys.ENTER)
            time.sleep(20)

            box = WebDriverWait(driver, 20).until(EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="page-content"]/div/div/div[2]/div[2]/div[6]/div/div[2]/div[5]/div/div[1]/div')))
            box.click()
            dropdown = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, '//*[@id="market-dashboard-categories"]')))
            num_options = 45 if group == "Dubai All" else 43
            for i in range(15, num_options):
                for _ in range(i):
                    dropdown.send_keys(Keys.ARROW_DOWN)
                    time.sleep(3)
                dropdown.send_keys(Keys.ENTER)
                time.sleep(3)
                selected_option = dropdown.get_attribute("value")
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "pacing-update-button"))).click()
                time.sleep(40)
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[@id=\'{"index":"compare_against_market","type":"filter-open-btn"}\']'))).click()
                time.sleep(5)
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'csv-future-price-occ'))).click()
                time.sleep(30)
                try:
                    WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, '/html/body/div/div/div[2]/div/div/div[2]/div[2]/div[6]/div/div[1]/div[1]/div[3]/div/div/div/div[1]/div/button'))).click()
                except:
                    pass
                add_plb_area_column(download_dir, selected_option)
    finally:
        driver.quit()


def concatenate_and_save_files():
    current_date = datetime.now().date()
    dfs = []
    for file in os.listdir(download_dir):
        file_path = os.path.join(download_dir, file)
        creation_date = datetime.fromtimestamp(os.path.getctime(file_path)).date()
        if file.startswith("FuturePriceOccupancy") and creation_date == current_date and os.path.getsize(file_path) > 200 * 1024:
            df = pd.read_csv(file_path)
            dfs.append(df)
    if dfs:
        union_df = pd.concat(dfs, ignore_index=True)
        average_df = union_df.groupby(['staydate', 'staydate_stly', 'plb_area']).mean().round(2)
        os.makedirs(output_directory, exist_ok=True)
        output_file_path = os.path.join(output_directory, f"union_{current_date}.csv")
        average_df.to_csv(output_file_path, index=False)
        logging.info(f"Union DataFrame saved as {output_file_path}")
    else:
        logging.info("No files found matching the criteria.")


def export_to_remote_db():
    try:
        local_conn = pymysql.connect(**local_db_config)
        remote_conn = pymysql.connect(**remote_db_config)
        try:
            cursor = local_conn.cursor()
            cursor.execute("SHOW TABLES LIKE 'union_%'")
            tables = cursor.fetchall()
            union_tables = [table[0] for table in tables]

            for table_name in union_tables:
                cursor.execute(f"SELECT * FROM `{table_name}`")
                rows = cursor.fetchall()
                if not rows:
                    logging.info(f"No data found in table {table_name} to transfer.")
                    continue
                cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                columns = [col[0] for col in cursor.fetchall()]
                data = [dict(zip(columns, row)) for row in rows]
                data_json = json.dumps(data)
                data = json.loads(data_json)
                if not data:
                    logging.info(f"No data to insert into {table_name} on remote database.")
                    continue
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{REMOTE_DB_NAME}`.`{table_name}` (
                    ` StayDate` TEXT, `StayDate STLY` TEXT, `plb_area` TEXT, `NumListings` FLOAT, `CalendarUnavailable` FLOAT,
                    `Booked` FLOAT, `Booked STLY` TEXT, `Booked YOY(%)` FLOAT, `Pickup (last week)` TEXT, `Pickup STLY (last week)` TEXT,
                    `Pickup (last week) YOY(%)` FLOAT, `Last Seen Price` TEXT, `Revenue` FLOAT, `BookedPrice` FLOAT, `ADR` TEXT,
                    `ADR STLY` TEXT, `ADR YOY(%)` FLOAT, `RevPAR` TEXT, `RevPAR STLY` TEXT, `RevPAR YOY(%)` TEXT, `MarketOccupancy` TEXT,
                    `MarketOccupancy_STLY` TEXT, `MarketOccupancy_LY` TEXT, `Market Occupancy YOY(%)` TEXT, `MarketPickup7` TEXT,
                    `MarketPickup7_STLY` TEXT, `Market Pickup (last week) YOY(%)` TEXT, `MarketMedianPrice` TEXT, `Market25PercentilePrice` TEXT,
                    `Market75PercentilePrice` TEXT, `Market90PercentilePrice` TEXT, `MarketADR` TEXT, `MarketADR_STLY` TEXT,
                    `MarketADR_LY` TEXT, `MarketRevpar` TEXT, `MarketRevpar_STLY` TEXT, `MarketRevpar_LY` TEXT
                )"""
                cursor.execute(create_table_query)
                remote_conn.commit()
                insert_query = f"INSERT INTO `{REMOTE_DB_NAME}`.`{table_name}` VALUES ({', '.join(['%s'] * len(data[0]))})"
                rows_list = [(row[' StayDate'], row['StayDate STLY'], row['plb_area'], row['NumListings'], row['CalendarUnavailable'],
                              row['Booked'], row['Booked STLY'], row['Booked YOY(%)'], row['Pickup (last week)'], row['Pickup STLY (last week)'],
                              row['Pickup (last week) YOY(%)'], row['Last Seen Price'], row['Revenue'], row['BookedPrice'], row['ADR'],
                              row['ADR STLY'], row['ADR YOY(%)'], row['RevPAR'], row['RevPAR STLY'], row['RevPAR YOY(%)'], row['MarketOccupancy'],
                              row['MarketOccupancy_STLY'], row['MarketOccupancy_LY'], row['Market Occupancy YOY(%)'], row['MarketPickup7'],
                              row['MarketPickup7_STLY'], row['Market Pickup (last week) YOY(%)'], row['MarketMedianPrice'], row['Market25PercentilePrice'],
                              row['Market75PercentilePrice'], row['Market90PercentilePrice'], row['MarketADR'], row['MarketADR_STLY'],
                              row['MarketADR_LY'], row['MarketRevpar'], row['MarketRevpar_STLY'], row['MarketRevpar_LY']) for row in data]
                remote_cursor = remote_conn.cursor()
                remote_cursor.executemany(insert_query, rows_list)
                remote_conn.commit()
                logging.info(f"Data transferred from {table_name} to remote database.")
        except Exception as e:
            logging.error(f"Error during data transfer: {str(e)}")
        finally:
            local_conn.close()
            remote_conn.close()
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")


def send_email_notification():
    send_email('shubhamtwr2@gmail.com',  'DB Transfer Success', 'DB Transfer was successful')


def execute_full_process():
    fetch_and_download_reports()
    concatenate_and_save_files()
    export_to_remote_db()
    send_email_notification()

# Schedule the script to run at the desired time
schedule.every().day.at("23:15").do(execute_full_process)

while True:
    schedule.run_pending()
    time.sleep(1)
