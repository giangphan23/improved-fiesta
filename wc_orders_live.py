import re
import time
import json
import datetime
import requests
import pandas as pd
from woocommerce import API
import logging
import os
PATH = os.getcwd()

FILENAME = "wc_orders"
AFTER = None  # None if not filtered

# FILENAME = "wc_orders_from_220301"
# AFTER = "2022-02-28T23:59:59"  # None if not filtered

URL = r"https://cares.docosan.com/"

# API key for updates through Python
CONSUMER_KEY = "ck_e63ba1eb64865a0ad289d1b99e52350c7eda781e"
CONSUMER_SECRET = "cs_f6fbb4317cece55abbbd2b3ff5c5e086124a9fb1"

VERSION = "wc/v3"
PATTERN = "page=\d+"
PHONE_REGEX_PATTERN = r"(\d{10,}|\d{9,}|\d{3,}\s\d{7,}|\d{4,}\s\d{6,}|\d{4,}\s\d{3,}\s\d{3,}|\d{3,}\s\d{3,}\s\d{2,}\s\d{2,}|84\d{9}|84\s\d{2}\s\d{7}|84\s\d{2}\s\d{3}\s\d{2}\s\d{2}|\d{4}.\d{3}.\d{3})"
logging.basicConfig(
    filename=f"{PATH}/wc.log",
    format="%(asctime)s%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S.",
    level=logging.DEBUG,
)


def get_page(wcapi: API, page: int = 1):
    res = requests.Response()
    start = time.time()
    while res.status_code != 200:
        res = wcapi.get("orders", params={"page": page, "after": AFTER})
        # logging.info(res.status_code)
    end = time.time()
    return end - start, res

def get_pages(wcapi: API, limit: int = 1000000):
    req_time, response = get_page(wcapi)
    pages_content = response.json()
    total_req_time = req_time
    logging.info(f"Pulled 1 page in {req_time:.3f} seconds")
    next_page = -1
    while "next" in list(response.links.keys()) and next_page < limit:
        next_page = int(
            re.search(PATTERN, response.links["next"]["url"]).group().split("=")[-1]
        )
        req_time, response = get_page(wcapi, next_page)
        pages_content += response.json()
        total_req_time += req_time
        logging.info(
            f"Pulled {next_page} pages in {total_req_time:.3f} seconds (last page: {req_time:.3f} seconds, average: {total_req_time / next_page:.3f} seconds/page)"
        )
    return pages_content

# extract raw_phone_number from billing if exists, otherwise extract from shipping
def raw_phone_number(billing, shipping):
    return billing if re.search(PHONE_REGEX_PATTERN, billing) else shipping

def pivot_billing(df: pd.DataFrame):
    all_fields_series = []
    billing_column_names = None
    for row in range(df.shape[0]):
        billing = df["billing"].values[row]
        if isinstance(billing, str):
            billing = eval(billing)
        if not isinstance(billing, list):
            billing = [billing]

        billing_df = pd.json_normalize(billing)
        try:
            # logging.debug(df.at[row, "billing"], df.at[row, "shipping"])

            phone_num = re.sub(
                r"\D",
                " ",
                raw_phone_number(
                    json.dumps(df.at[row, "billing"]),
                    json.dumps(df.at[row, "shipping"]),
                ),
            )
            # logging.debug(phone_num)

            phone_num = (
                phone_num.replace(" ", "")
                if re.search(r"(\+\d{2})", phone_num)
                else phone_num
            )
            # logging.debug(phone_num)

            phone_num = (
                re.search(PHONE_REGEX_PATTERN, phone_num).group(0)
                if re.search(PHONE_REGEX_PATTERN, phone_num)
                else phone_num
            )
            # logging.debug(phone_num)

            phone_num = (
                "84" + phone_num
                if len(phone_num) < 10 and not phone_num.startswith("0")
                else phone_num
            )
            # logging.debug(phone_num)

            phone_num = (
                re.sub(r"(^0)|(^9)|(^84)", "(+84)", phone_num)
                if re.search(r"(^0)|(^9)|(^84)", phone_num)
                else re.sub(r"(^44)", "(+44)", phone_num)
                if re.search(r"(^44)", phone_num)
                else re.sub(r"(^19)", "(+19)", phone_num)
                if re.search(r"(^19)", phone_num)
                else ""
            )
            # logging.debug(phone_num)

            phone_num = (
                re.sub(r"(\s|\.)", "", phone_num)
                if re.search(r"(^[(+84)])|(^[(+44)])|(^[(+19)])", phone_num)
                else phone_num
            )
            logging.debug(phone_num)
            customer_id = (
                (billing[0]["last_name"] + " " + billing[0]["first_name"]).upper()
                if phone_num == ""
                else phone_num
            )
            billing_df["customer_id_"] = [customer_id]
        except:
            logging.error("", exc_info=True)

        for i in range(len(billing)):
            all_fields_series.append(
                pd.concat([df.loc[row], pd.Series(data=billing_df.loc[i].values)])
            )

        if row == 0:
            billing_column_names = ("billing_" + billing_df.columns.values).tolist()

    all_fields_df = pd.DataFrame(all_fields_series)
    all_fields_df.columns = df.columns.values.tolist() + billing_column_names

    return all_fields_df

def pivot_line_items(df: pd.DataFrame):
    all_fields_series = []
    line_items_column_names = None
    for row in range(df.shape[0]):
        line_items = df["line_items"].values[row]
        if isinstance(line_items, str):
            line_items = eval(line_items)
        # if row == 0:
        #     logging.info(line_items)

        line_items_df = pd.json_normalize(line_items)

        for i in range(len(line_items)):
            all_fields_series.append(
                pd.concat([df.loc[row], pd.Series(data=line_items_df.loc[i].values)])
            )

        if row == 0:
            line_items_column_names = (
                "line_items_" + line_items_df.columns.values
            ).tolist()

    all_fields_df = pd.DataFrame(all_fields_series)
    all_fields_df.columns = df.columns.values.tolist() + line_items_column_names

    return all_fields_df

def filter_test_and_internal(df):
    test_and_internal = []
    for row in range(df.shape[0]):
        billing = df["billing"].values[row]
        shipping = df["shipping"].values[row]
        if (
            "test" in billing["first_name"].lower()
            or "test" in billing["last_name"].lower()
            or "@test" in billing["email"].lower()
            or "test" in billing["email"].lower()
            or "docosan" in billing["company"].lower()
            or "Tuong Test" in billing["company"]
            or "Thành Test" in billing["company"]
            or "abcd" in billing["company"]
            or "test" in shipping["first_name"].lower()
            or "test" in shipping["last_name"].lower()
            or "docosan" in shipping["company"].lower()
        ):
            test_and_internal.append("Yes")
        else:
            test_and_internal.append("No")

    df["test_and_internal"] = pd.Series(test_and_internal)
    return df


logging.info("PULLING...")
wcapi = API(
    url=URL,
    consumer_key=CONSUMER_KEY,
    consumer_secret=CONSUMER_SECRET,
    version=VERSION,
    verify_ssl=False,
    timeout=100,
)
# all_pages = get_pages(wcapi, limit=3)
all_pages = get_pages(wcapi)

logging.info("PREPROCESSING...")
dfw = pd.DataFrame.from_records(all_pages)
dfw1 = filter_test_and_internal(dfw)
dfw1 = pivot_line_items(dfw1)
dfw2 = pivot_billing(dfw1)

now = datetime.datetime.now().strftime(r"%y%m%d_%H%M%S")
# dfw2.to_csv(f"{PATH}/data/{FILENAME}.csv", index=True)


##########################################################################
# automate updating wc_order
##########################################################################


# drop columns containing invalid values
col_to_drop = [
    # dicts:
    'billing', 'shipping','meta_data','line_items','_links',
    # empty lists:
    'tax_lines', 'shipping_lines', 'fee_lines', 'coupon_lines', 'refunds', 'line_items_taxes', 'line_items_meta_data'
    ]
dfw2 = dfw2.drop(col_to_drop, axis=1)

# ADD F1 COLUMN (aka ROW INDEX)
dfw3 = dfw2.reset_index().rename({'index':''}, axis=1)
dfw3.head()

from docosan_module import *
sheet_url = "https://docs.google.com/spreadsheets/d/1wvHxPkZ8wXgplRJDbdndyJjKbS7E2ZEhDTiOAQo6umk/edit#gid=1697317978"
update_gsheet(sheet_url, dfw3)
