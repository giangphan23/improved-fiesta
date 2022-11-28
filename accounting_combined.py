import numpy as np
import pandas as pd
from docosan_module import *


# extract data
apt_file_path = 'SQL/Appointments.sql'
df_apt = sql_to_df(apt_file_path)

inv_file_path = 'SQL/Epaid Appointment Invoices.sql'
df_inv = sql_to_df(inv_file_path)

agent_file_path = 'SQL/Appointments by Agent.sql'
df_agent = sql_to_df(agent_file_path)

# join data
df1 = df_apt.merge(df_inv, how='left', left_on='Appointment ID', right_on='appointment_id').merge(df_agent, how='left', left_on='Appointment ID', right_on='appointment_id')
df1.sort_values('Appointment ID', ascending=False, inplace=True)


# rename
df2 = df1.rename({
    'Appointment ID': 'Apt ID',
    'Requester': 'Booked for',
    'Patient Name': 'Patient',
    'agent_name': 'Agent',
    'Phone Number': 'Phone Number',
    'Clinic Name': 'Clinic',
    'Doctor Name': 'Doctor',
    'Create Date': 'Requested At',
    'Status': 'Appointment Status',
    'Appointment Type': 'Payment Type',
    'original_payment_method': 'Original Payment Method',
    'epaid_id': 'Epaid Id',
    'original_service_type': 'Original Service Type',
    'original_fee': 'Original Fee',
    'original_fee_status': 'Original Fee Status',
    'original_fee_details': 'Original Fee Details',
    'extra_fee_requested': 'Extra Fee Requested',
    'extra_fee_paid': 'Extra Fee Paid',
    'extra_fee_details': 'Extra Fee Details'
    }, axis=1)

# create column 'LK Fee Receiver'
cond_1 = df2['Clinic ID'] == 744
cond_2 = df2['Reason'].str.lower().str.contains('thi(e|ệ)n', na=False, regex=True)
cond_3 = df2['Reason'].str.lower().str.contains('ph(a|á)t', na=False, regex=True)

df2['LK Fee Receiver'] = ''
df2.loc[cond_1, 'LK Fee Receiver'] = 0
df2.loc[cond_1 & cond_2, 'LK Fee Receiver'] = 'Thiện'
df2.loc[cond_1 & cond_3, 'LK Fee Receiver'] = 'Phát'

# filters
col = ['Apt ID', 'Booked for', 'Agent', 'Patient', 'Phone Number',
       'Patient Birthday', 'Patient Gender', 'Clinic ID', 'Clinic', 'Doctor',
       'Appointment Date', 'Appointment Time', 'Reason',
       'LK Fee Receiver',
       'Requested At', 'Appointment Mode', 'Raw Status', 'Appointment Status',
       'Show/No-show', 'Original Payment Method', 'Epaid Id', 'Original Fee',
       'Extra Fee Paid', 'Extra Fee Requested', 'Original Fee Status',
       'Original Service Type', 'Original Fee Details', 'Extra Fee Details',
       'Payment Type']

df3 = df2[col]

# format date
df3['Appointment Date'] = pd.to_datetime(df3['Appointment Date']).dt.strftime('%m/%d/%Y')
df3['Requested At'] = pd.to_datetime(df3['Requested At']).dt.strftime('%m/%d/%Y')

df3.to_excel('G:/My Drive/data/finance_data/accounting_combined.xlsx', index=False)
