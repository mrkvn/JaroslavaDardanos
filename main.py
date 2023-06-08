import os
import sqlite3
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def download_data(url):
    options = Options()
    prefs = {"download.default_directory": os.path.abspath(os.getcwd())}
    options.add_experimental_option("prefs", prefs)
    # options.add_experimental_option("detach", True)
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    button = driver.find_element("xpath", '//*[@id="post-252"]//*/a')
    button.click()

    number_of_seconds = 0
    # check if excel is already downloaded
    while True:
        for i, filename in enumerate(os.listdir(".")):
            # if after roughly 10 seconds, the file did not download successfully, try to download again.
            if number_of_seconds >= 10:
                driver.quit()
                if filename.__contains__("xlsx"):
                    os.remove(filename)
                    print("Trying again...")
                    download_data(url)

            if filename.endswith(".xlsx"):
                driver.quit()
                return filename
            if i == len(os.listdir(".")) - 1:
                time.sleep(1)
                number_of_seconds += 1
                continue


def do_pivot(filename):
    df = pd.read_excel(filename, sheet_name="data")
    df = (
        df.groupby("Platform (Northbeam)")[
            ["Spend", "Attributed Rev (1d)", "Imprs", "Visits", "New Visits", "Transactions (1d)", "Email Signups (1d)"]
        ]
        .sum()
        .sort_values(by="Attributed Rev (1d)", ascending=False)
    )
    return df


def sqlite_create_table():
    sql_create_table = """
        CREATE TABLE IF NOT EXISTS pivot_table (
            platform_northbeam text,
            spend real,
            attributed_rev_1d real,
            imprs integer,
            visits integer,
            new_visits integer,
            transactions_1d real,
            email_signups_1d real
            )
    """
    c.execute(sql_create_table)
    conn.commit()


def sqlite_insert_pivot_table(pivot_table):
    for row in pivot_table.itertuples():
        sql_insert = """
        INSERT INTO pivot_table VALUES (:platform_northbeam, :spend, :attributed_rev_1d, :imprs, :visits, :new_visits, :transactions_1d, :email_signups_1d)
        """
        c.execute(
            sql_insert,
            {
                "platform_northbeam": row[0],
                "spend": row[1],
                "attributed_rev_1d": row[2],
                "imprs": row[3],
                "visits": row[4],
                "new_visits": row[5],
                "transactions_1d": row[6],
                "email_signups_1d": row[7],
            },
        )
    conn.commit()


if __name__ == "__main__":
    url = "https://jobs.homesteadstudio.co/data-engineer/assessment/download/"
    print("Downloading data...")
    excel_file = download_data(url)
    pivot_table = do_pivot(excel_file)

    conn = sqlite3.connect("homesteadstudio.db")
    c = conn.cursor()
    print("Creating SQLITE Table...")
    sqlite_create_table()
    print("Inserting data to SQLITE DB")
    sqlite_insert_pivot_table(pivot_table)
    print("DONE :)")
