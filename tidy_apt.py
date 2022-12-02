import numpy as np
import pandas as pd

import docosan_module as domo

#####################################################################
#####################################################################
# EXTRACT
# invoices_path = 'SQL/Epaid Appointment Invoices.sql'
# invoices_df = domo.sql_to_df(invoices_path)

# apt_path = 'SQL/Appointments_finance.sql'
# apt_df = domo.sql_to_df(apt_path)

# agent_path = 'SQL/Appointments by Agent.sql'
# agent_df = domo.sql_to_df(agent_path)

# invoices_df.to_pickle('pickles/invoices_df.pkl')
# apt_df.to_pickle('pickles/apt_df.pkl')
# agent_df.to_pickle('pickles/agent_df.pkl')

invoices_df = pd.read_pickle('pickles/invoices_df.pkl')
apt_df = pd.read_pickle('pickles/apt_df.pkl')
agent_df = pd.read_pickle('pickles/agent_df.pkl')


#####################################################################
#####################################################################
# TRANSFORM


# join data
apt_info_df = apt_df.\
    join(invoices_df.set_index('appointment_id'), on='Appointment ID').\
    join(agent_df.set_index('appointment_id'), on='Appointment ID').\
    sort_values('Appointment ID', ascending=False)


# pivot order items
apt_info_df_with_items = apt_info_df[apt_info_df.item_detail.notna()].sort_values('Create Date')

item_df = domo.pivot_item_detail(apt_info_df_with_items)

apt_info_df_with_items_pivoted = apt_info_df_with_items.\
    join(item_df.set_index('item_apt_id'), on='Appointment ID')

apt_info_df_no_item = apt_info_df[apt_info_df.item_detail.isna()]

apt_info_df2 = pd.concat([apt_info_df_no_item, apt_info_df_with_items_pivoted])


# Type Report column
apt_info_df2['Type Report'] = domo.create_type_report(apt_info_df2)


# replace values in item_payment_method
apt_info_df2['item_payment_method'] = apt_info_df2['item_payment_method'].replace({
    'local_banking': 'Bank Transfer',
    'cod': 'COD',
    'zalopay_atm': 'Zalopay',
    'zalopay': 'Zalopay',
    'zalopay_cc': 'Zalopay',
    'momo': 'Momo',
    'onepay': 'Onepay',
    'onepay_atm': 'Onepay',
    'free_paid': 'Free',
    'mpp_guaranteed': 'MPP',
    'internet_banking': 'Zalopay'
    }).fillna('At Clinic')


# item grouping
group_df_filled = domo.load_gsheet('https://docs.google.com/spreadsheets/d/\
    1SoATnnqdqB66XN0En7ivt8T6odGynGWSNYajqar8O6k/').\
    iloc[:, [0,3]].dropna(subset='Appointment ID')

"""
# filter "Cares Actual" apt
col = ['Appointment ID', 'Reason', 'item_name']
cond1 = apt_info_df2['Type Report'].str.contains('Cares Actual')
cond2 = apt_info_df2['item_name_group'].isna()
group_df = apt_info_df2.loc[cond1 & cond2, col].\
    drop_duplicates(subset='item_name').sort_values('Appointment ID')

# upload to gsheet and fill in item groups manually
domo.update_gsheet('https://docs.google.com/spreadsheets/d/\
    1SoATnnqdqB66XN0En7ivt8T6odGynGWSNYajqar8O6k/', group_df)

# download filled data from gsheet
group_df_filled = domo.load_gsheet('https://docs.google.com/spreadsheets/d/\
    1SoATnnqdqB66XN0En7ivt8T6odGynGWSNYajqar8O6k/').iloc[:,[0,3]].dropna(subset='item_name_group')
"""
apt_info_df3 = apt_info_df2.join(group_df_filled.set_index('Appointment ID'),\
    on='Appointment ID')


# travel fee
patient_address_df = domo.get_travel_fee(apt_info_df3.\
    dropna(subset='Patient Address').drop_duplicates(subset='Appointment ID'))
apt_info_df3 = apt_info_df3.join(patient_address_df['fee_home_visit'],\
    on='Appointment ID')


#####################################################################
#####################################################################
# process apt data for finance
apt_final_df = pd.DataFrame()

apt_final_df['Order ID'] = 'A' + apt_info_df3['Appointment ID'].\
    astype(str)

apt_final_df['Source'] = 'Appointment'

apt_final_df['Patient ID'] = apt_info_df3['Patient ID'].\
    astype('Int64').astype(str)

apt_final_df['Phone Number'] = domo.clean_phone_number(
    apt_info_df3['Phone Number'].reset_index(drop=True))

apt_final_df['Doctor Name'] = np.where(
    apt_info_df3['graduate_id']!=13, apt_info_df3['Doctor Name'], '')

apt_final_df['Nurse Name'] = np.where(
    apt_info_df3['graduate_id']==13, apt_info_df3['Doctor Name'], '')

apt_final_df['Order Status'] = ''

apt_final_df['Item SKU'] = np.where(apt_info_df3['item_id'].isna(),
    '',
    apt_info_df3['Clinic ID'].astype('Int64').astype(str)
    + '_'
    + apt_info_df3['item_id'].astype('Int64').astype(str)
    )

apt_final_df['Item Unit Price'] = apt_info_df3['item_price'].\
    astype(float).astype('Int64').fillna(0)

apt_final_df['Item Quantity'] = apt_info_df3['item_quantity'].\
    astype(float).astype('Int64').fillna(0)

apt_final_df['Item Subtotal'] = apt_final_df['Item Unit Price'] \
    * apt_final_df['Item Quantity']

apt_final_df['Nurse Fee'] = apt_info_df3['Nurse Fee'].astype('Int64')

apt_final_df['Order Total'] = (
    apt_info_df3['original_fee'].astype(float).astype('Int64').fillna(0)
    + apt_info_df3['extra_fee_requested'].astype('Int64').fillna(0)
    )

#####################################################################
apt_final_df['Order Number'] = apt_info_df3['Appointment ID']
apt_final_df['Booked For'] = apt_info_df3['Requester']
apt_final_df['Agent ID'] = apt_info_df3['agent_id']
apt_final_df['Agent Name'] = apt_info_df3['agent_name']
apt_final_df['Booked By'] = apt_info_df3['Patient Name']
apt_final_df['Birthday'] = apt_info_df3['Patient Birthday']
apt_final_df['Gender'] = apt_info_df3['Patient Gender']
apt_final_df['Cluster ID'] = apt_info_df3['Cluster ID']
apt_final_df['Clinic ID'] = apt_info_df3['Clinic ID']
apt_final_df['Clinic Name'] = apt_info_df3['Clinic Name']
apt_final_df['Doctor ID'] = apt_info_df3['Doctor ID']
apt_final_df['Schedule Date'] = apt_info_df3['Appointment Date']
apt_final_df['Schedule Time'] = apt_info_df3['Appointment Time']
apt_final_df['Note'] = apt_info_df3['Reason']
apt_final_df['Delivery Status'] = apt_info_df3['Show/No-show']
apt_final_df['Create Date'] = apt_info_df3['Create Date']
apt_final_df['Mode'] = apt_info_df3['Appointment Mode']
apt_final_df['Apt Status'] = apt_info_df3['Status']
apt_final_df['Lab Status'] = apt_info_df3['Lab Status']
apt_final_df['Payment Method'] = apt_info_df3['item_payment_method']
apt_final_df['Payment ID'] = apt_info_df3['epaid_id']
apt_final_df['Payment Date'] = apt_info_df3['original_payment_date']
apt_final_df['Payment Status'] = apt_info_df3['item_payment_status']
apt_final_df['Item ID'] = apt_info_df3['item_id']
apt_final_df['Item Name VI'] = apt_info_df3['item_name']
apt_final_df['Item Name Group'] = apt_info_df3['item_name_group']
apt_final_df['B2B'] = apt_info_df3['B2B']
apt_final_df['Type Report'] = apt_info_df3['Type Report']
apt_final_df['Platform'] = apt_info_df3['Platform']
apt_final_df['Cluster Name'] = apt_info_df3['Cluster Name']
apt_final_df['Travel Fee'] = apt_info_df3['fee_home_visit']

apt_final_df['Shipping Total'] = np.nan
apt_final_df['Tax Rate'] = np.nan
apt_final_df['Tax Total'] = np.nan
apt_final_df['Discount Rate'] = np.nan
apt_final_df['Discount Total'] = np.nan
apt_final_df['COGS Per Unit'] = np.nan
apt_final_df['COGS Total (including packaging)'] = np.nan
apt_final_df['Commission Rate (%) for Docosan'] = np.nan
apt_final_df['DCS Commission Revenue (VAT excluded)'] = np.nan
apt_final_df['Reimbursement Per Unit'] = np.nan
apt_final_df['Reimbursement Total'] = np.nan
apt_final_df['Telemed Fee Per Unit'] = np.nan
apt_final_df['Telemed Fee - Total'] = np.nan
apt_final_df['Margin'] = np.nan
apt_final_df['Item Category ID'] = ''
apt_final_df['Currency'] = ''
apt_final_df['Shipping Method'] = ''
apt_final_df['Company Name'] = ''
apt_final_df['Email'] = ''
apt_final_df['Address'] = ''

#####################################################################
#####################################################################
# LOAD
domo.update_gsheet('https://docs.google.com/spreadsheets/d/\
    1toxh7WoGWurp1F0R_IEhb_8KU82twtE7EClSRQMZmu4/', apt_final_df)
