import json
import logging
import os
import re

import pandas as pd
import phonenumbers as pn
import numpy as np
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

# clean phone number
def clean_phone_number(series):
    # series = woo_df_1['_billing_phone']
    # series = df_apt_ready['Phone Number'].reset_index(drop=True)
    # series = pd.Series(['12027953213', '447975777666', '0707821006', np.nan])

    # extract phone_number from ser using PHONE_REGEX_PATTERN; remove non-numeric char
    phone_number = series.str.extract(PHONE_REGEX_PATTERN, expand=False).str.replace('\D', '', regex=True)

    # count char
    phone_len = phone_number.str.len()

    # phone country code
    conditions = [
        (phone_len > 0) & (phone_len <= 10) | phone_number.str.startswith('0'),
        phone_number.str.startswith('1').fillna(False)
        ]
    choices = [
        '84', # VN
        '1' # US & some other countries
        ]
    phone_code = pd.Series(np.select(conditions, choices, default=phone_number.str[:2]), index=series.index)

    # phone country name
    phone_country = pd.Series(phone_code, index=series.index).fillna(0).astype(int).apply(lambda x: pn.region_code_for_country_code(x))

    # parse & format phone number
    ls=[]
    for r in series.index:
        # r = 0
        p_cntr = phone_country.loc[r]
        p_code = phone_code.loc[r]
        p_n = phone_number.loc[r]
        if (p_cntr != 'ZZ') & (p_cntr != ''):
            try:
                p_cleaned = pn.format_number(pn.parse(p_n, p_cntr), pn.PhoneNumberFormat.NATIONAL)
            except:
                p_cleaned = ''
            ls.append("(+{}) {}".format(p_code, p_cleaned))
        else:
            ls.append('')

    return ls


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
            or "Th??nh Test" in billing["company"]
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


def update_gsheet(sheet_url, df, sheet_title=None, string_escaping='default'):
    if sheet_title is None: # default: open & clear sheet1
        worksheet = get_google_cred().open_by_url(sheet_url).get_worksheet(0)
    else: # use sheet_title if provided
        worksheet = get_google_cred().open_by_url(sheet_url).worksheet(sheet_title)

    # clear sheet before updating
    worksheet.clear()

    # update df to gsheet
    set_with_dataframe(worksheet, df, string_escaping=string_escaping)


def load_gsheet(sheet_url):
    # open & get data from sheet
    worksheet = get_google_cred().open_by_url(sheet_url).get_worksheet(0)
    df = get_as_dataframe(worksheet)
    return df


def append_gsheet(sheet_url, df):
    # open sheet by url
    worksheet = get_google_cred().open_by_url(sheet_url).get_worksheet(0)

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


# execute single SQL query
def single_SQL_query_to_df(command):
    open_ssh_tunnel()
    mysql_connect()
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


# Disconnect and close the tunnel
def mysql_disconnect():
    """Closes the MySQL database connection.
    """

    connection.close()

def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """

    tunnel.close


########################################################
# extract data from WP/Woo MySQL database
########################################################

def extract_woo(command):
    connection = pymysql.connect(
        host='13.215.162.252',
        user='dcscares',
        passwd='docosancares2022',
        db='wordpress1'
    )

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





#######################################


def pivot_item_detail(df):
    # initiate df to contain pivoted data
    item_df = pd.DataFrame()

    for _, r in df.iterrows():
        # dict: i = 31607; r = df.loc[i]
        # tuple: i = 34181; r = df.loc[i]
        # tuple: i = 35401; r = df.loc[i]

        # initiate df to contain pivoted data
        item_ori_df = pd.DataFrame()
        item_sale_df = pd.DataFrame()
        item_extra_df = pd.DataFrame()

        # put each item, its payment_method & status into
        null = None
        apt_id = r['Appointment ID']
        items = eval(r['item_detail'])
        payment_methods = r['payment_method'].split(',') if r['payment_method'] else ''
        payment_statuses = r['status'].split(',')

        if isinstance(items, dict):
            items = tuple((items,))

        for item, payment_method, payment_status in zip(items, payment_methods, payment_statuses): #print(item, payment_method, payment_status, sep='\n\n')
            try:
                item_ori_ = pd.json_normalize(item, ['items','services'])
                item_ori_['apt_id'] = apt_id
                item_ori_['payment_method'] = payment_method
                item_ori_['payment_status'] = payment_status
                item_ori_df = pd.concat([item_ori_df, item_ori_])
            except:
                pass

            try:
                item_sale_ = pd.json_normalize(item, ['items','sale_services'])
                item_sale_['apt_id'] = r['Appointment ID']
                item_sale_['payment_method'] = payment_method
                item_sale_['payment_status'] = payment_status
                item_sale_df = pd.concat([item_sale_df, item_sale_])
            except:
                pass

            try:
                item_extra_ = item['items']['services']['extra']
                item_extra_ = pd.json_normalize(item_extra_)
                item_extra_['id'] = 0
                item_extra_['apt_id'] = r['Appointment ID']
                item_extra_['payment_method'] = payment_method
                item_extra_['payment_status'] = payment_status
                item_extra_['quantity'] = 1
                item_extra_df = pd.concat([item_extra_df, item_extra_])
            except:
                pass

        item_df = pd.concat([item_df, item_ori_df, item_sale_df, item_extra_df], axis=0)
    item_df = item_df.add_prefix('item_')
    return item_df


def create_type_report(df):
    conditions = [
        (df['Cluster ID']!=299) & (df['Status'].str.contains('Confirmed|Auto rejected')),
        (df['Cluster ID']!=299) & ~(df.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
        (df['Clinic ID']==615) & (df['Status'].str.contains('Confirmed|Auto rejected')),
        (df['Clinic ID']==615) & ~(df.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
        (df['Clinic ID'].isin([684,744,760,761])) & (df['Status'].str.contains('Confirmed|Auto rejected')),
        (df['Clinic ID'].isin([684,744,760,761])) & ~(df.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
        ]
    choices = [
        'Appointment Actual',
        'Appointment Adjustment',
        'None Actual',
        'None Adjustment',
        'Cares Actual',
        'Cares Adjustment',
        ]
    return np.select(conditions, choices, default='')


def get_travel_fee(df):
    null = None
    df_patient_address = pd.DataFrame()
    for _, r in df.iterrows(): #print(i, r)
        if r['Patient Address']:
            patient_address = pd.json_normalize(eval(r['Patient Address']))
            patient_address['apt_id'] = r['Appointment ID']
            df_patient_address = pd.concat([df_patient_address, patient_address])
        else: pass
    df_patient_address = df_patient_address.drop_duplicates('apt_id').set_index('apt_id')
    return df_patient_address




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

