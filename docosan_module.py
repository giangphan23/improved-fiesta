import json
import logging
import os
import re

import pandas as pd
import pymysql
import requests
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from woocommerce import API

########################################################
# get data from Woo via API
########################################################

def get_page(table = 'orders'):
    wcapi = API(
        url=r"https://cares.docosan.com/",
        consumer_key="ck_e63ba1eb64865a0ad289d1b99e52350c7eda781e",
        consumer_secret="cs_f6fbb4317cece55abbbd2b3ff5c5e086124a9fb1",
        version="wc/v3",
        verify_ssl=False,
        timeout=100)
    res = requests.Response()
    while res.status_code != 200:
        res = wcapi.get(table, params={"page": 1, "after": None})
    return res

def get_pages(limit: int = 1000000, table = 'orders'):
    response = get_page()
    pages_content = response.json()
    next_page = -1
    while "next" in list(response.links.keys()) and next_page < limit:
        next_page = int(
            re.search("page=\d+", response.links["next"]["url"]).group().split("=")[-1]
        )
        response = get_page(table)
        pages_content += response.json()
    return pages_content


########################################################
# processing Woo order data
########################################################

PHONE_REGEX_PATTERN = r"(\d{10,}|\d{9,}|\d{3,}\s\d{7,}|\d{4,}\s\d{6,}|\d{4,}\s\d{3,}\s\d{3,}|\d{3,}\s\d{3,}\s\d{2,}\s\d{2,}|84\d{9}|84\s\d{2}\s\d{7}|84\s\d{2}\s\d{3}\s\d{2}\s\d{2}|\d{4}.\d{3}.\d{3})"

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

########################################################
# update to Google Sheets
########################################################
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe


# authen google account
def get_google_cred():
    cred_file_path = os.path.join(os.getcwd() + '/Lib/site-packages/gspread/credentials.json')
    gc = gspread.oauth(credentials_filename=cred_file_path)
    return gc


def update_gsheet(sheet_url, df, string_escaping='default'):
    # open & clear sheet1
    worksheet = get_google_cred().open_by_url(sheet_url).get_worksheet(0)
    worksheet.clear()

    # update df to gsheet
    set_with_dataframe(worksheet, df, string_escaping=string_escaping)


def load_gsheet(sheet_url):
    # open & get data from sheet
    worksheet = get_google_cred().open_by_url(sheet_url).get_worksheet(0)
    df = get_as_dataframe(worksheet)
    return df


def append_gsheet(url, df):
    # open sheet by url
    worksheet = get_google_cred().open_by_url(url).get_worksheet(0)

    # append rows from df to end of sheet
    worksheet.append_rows(df.fillna('').values.tolist())


########################################################
# extract data from MySQL database
########################################################

# Configure credentials
ssh_host = '18.138.166.48'
ssh_username = 'root'
ssh_password = 'dcsdevelopserver'
database_username = 'metabase2'
database_password = 'metabase2!'
database_name = 'docosandynamic'
localhost = 'docosandynamic.c1lsds8lkx7p.ap-southeast-1.rds.amazonaws.com'

# Open an SSH tunnel
def open_ssh_tunnel(verbose=True):
    """Open an SSH tunnel and connect using a username and password.

    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """

    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address = (localhost, 3306)
    )

    tunnel.start()

# Connect to MySQL via the SSH tunnel
def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection

    :return connection: Global MySQL database connection
    """

    global connection

    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )


# Run SQL query
def sql_to_df(sql_file_path):

    # connect db
    open_ssh_tunnel()
    mysql_connect()

    # read sql file
    with open(sql_file_path, "r") as sql_file:
        sqlFile = sql_file.read()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';\n')

    # execute each command in order
    try:
        for command in sqlCommands:
            # print(command)
            if command != '':
                cursor = connection.cursor()
                cursor.execute(command)
            else: pass
            # print(cursor.description)
        col_name = [col[0] for col in cursor.description]
        values = cursor.fetchall()
    except Exception as e:
        print('This error can be ignored! ' + str(e))
        raise

    # convert to df
    df = pd.DataFrame(values, columns=col_name)

    # disconnect
    mysql_disconnect()
    close_ssh_tunnel()

    return df


# Connect to WooCommerce MySQL database
def wooDB_connect():

    global connection

    connection = pymysql.connect(
        host='13.215.162.252',
        user='dcscares',
        passwd='docosancares2022',
        db='wordpress1'
    )


# execute single SQL query
def single_SQL_query_to_df(command):
    try:
        if command != '':
            cursor = connection.cursor()
            cursor.execute(command)
        else: pass
        col_name = [col[0] for col in cursor.description]
        values = cursor.fetchall()
    except Exception as e:
        print('This error can be ignored! ' + str(e))
        raise

    # convert to df
    df = pd.DataFrame(values, columns=col_name)
    return df


# Run your SQL query
def woo_sql_to_df(sql_file_path):
    """Runs a given SQL query via the global database connection.

    :param sql: MySQL query
    :return: Pandas dataframe containing results
    """

    # connect db
    wooDB_connect()

    # read sql file
    fd = open(sql_file_path, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';\n')

    # execute each command in order
    try:
        for command in sqlCommands:
            # print(command)
            if command != '':
                cursor = connection.cursor()
                cursor.execute(command)
            else: pass
            # print(cursor.description)
        col_name = [col[0] for col in cursor.description]
        values = cursor.fetchall()
    except Exception as e:
        print('This error can be ignored! ' + str(e))
        raise

    # convert to df
    df = pd.DataFrame(values, columns=col_name)

    # disconnect
    mysql_disconnect()

    return df



# Disconnect and close the tunnel
def mysql_disconnect():
    """Closes the MySQL database connection.
    """

    connection.close()

def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """

    tunnel.close

#######################################33

# pivoting original_fee_details
def pivot_original_fee_details(df: pd.DataFrame):
    df = df.set_index('Appointment ID')
    ls = []
    for row in range(len(df)):
        item = df.iloc[row]['original_fee_details']
        if isinstance(item, str):
            null = None
            item = eval(item)
            item_df = pd.json_normalize(item)
            item_df['apt_id'] = df.iloc[row].name
            ls.append(item_df)
    df_pivoted = pd.concat(ls).add_prefix('item_')
    df_result = df.join(df_pivoted.set_index('item_apt_id'), how='left').reset_index(
    ).drop('original_fee_details', axis=1).rename(columns={'index': 'Appointment ID'})
    return df_result

# pivoting shipping
def pivot_shipping(df: pd.DataFrame):
    ls = []
    for row in range(len(df)):
        # row=0
        shipping_item = df.iloc[row]['shipping']
        if isinstance(shipping_item, str):
            null = None
            shipping_item = eval(shipping_item)
            shipping_df = pd.json_normalize(shipping_item)
            shipping_df['id'] = df.iloc[row].name
            ls.append(shipping_df)
    df_pivoted = pd.concat(ls).add_prefix('shipping_')
    df_result = df.join(df_pivoted.set_index('shipping_id'), how='left').drop('shipping', axis=1)
    return df_result

# pivoting shipping_lines
def pivot_shipping_lines(df: pd.DataFrame):
    ls = []
    for row in range(len(df)):
        # row=0
        shipping_item = df.iloc[row]['shipping_lines']
        if isinstance(shipping_item, str):
            null = None
            shipping_item = eval(shipping_item)
            shipping_df = pd.json_normalize(shipping_item)
            shipping_df['id'] = df.iloc[row].name
            ls.append(shipping_df)
    df_pivoted = pd.concat(ls).add_prefix('shipping_lines_')
    df_result = df.join(df_pivoted.set_index('shipping_lines_id'), how='left').drop('shipping_lines', axis=1)
    return df_result
