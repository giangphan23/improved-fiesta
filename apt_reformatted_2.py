import numpy as np
import pandas as pd
import docosan_module as domo

# extract data
inv_file_path = 'SQL/Epaid Appointment Invoices.sql'
df_inv = domo.sql_to_df(inv_file_path)

apt_file_path = 'SQL/Appointments_finance.sql'
df_apt = domo.sql_to_df(apt_file_path)

agent_file_path = 'SQL/Appointments by Agent.sql'
df_agent = domo.sql_to_df(agent_file_path)

# join data
df1 = df_apt.merge(df_inv, how='left', left_on='Appointment ID', right_on='appointment_id').merge(df_agent, how='left', left_on='Appointment ID', right_on='appointment_id')
df1.sort_values('Appointment ID', ascending=False, inplace=True)

# pivot each line in original_fee_details into 1 item per row
# df2 = domo.pivot_original_fee_details(df1)

#####################################################################

# Type Report column
conditions = [
    (df2['Cluster ID']!=299) & (df2['Status'].str.contains('Confirmed|Auto rejected')),
    (df2['Cluster ID']!=299) & ~(df2.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
    (df2['Clinic ID']==615) & (df2['Status'].str.contains('Confirmed|Auto rejected')),
    (df2['Clinic ID']==615) & ~(df2.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
    (df2['Clinic ID'].isin([684,744,760,761])) & (df2['Status'].str.contains('Confirmed|Auto rejected')),
    (df2['Clinic ID'].isin([684,744,760,761])) & ~(df2.fillna('')['Status'].str.contains('Confirmed|Auto rejected')),
    ]
choices = [
    'Appointment Actual',
    'Appointment Adjustment',
    'None Actual',
    'None Adjustment',
    'Cares Actual',
    'Cares Adjustment',
    ]
df2['Type Report'] = np.select(conditions, choices, default='')


# replace values in original_payment_method
df2['original_payment_method'] = df2['original_payment_method'].replace({
    'Bank transfer': 'Bank Transfer',
    'cod': 'COD',
    'internet_banking': 'Zalopay',
    'Onepay': 'OnePay'
}).fillna('At Clinic')


# item grouping
col = ['Appointment ID', 'Reason', 'item_name']
gr1 = df2.loc[df2['Type Report'].str.contains('Cares Actual'), col].drop_duplicates(subset='item_name').sort_values('Appointment ID')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1SoATnnqdqB66XN0En7ivt8T6odGynGWSNYajqar8O6k/', gr1) # manual grouping in gsheet
gr1_filled = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1SoATnnqdqB66XN0En7ivt8T6odGynGWSNYajqar8O6k/').iloc[:, 2:4].dropna(subset='item_name')
df2 = pd.merge(df2, gr1_filled, how='left', left_on='item_name', right_on='item_name', )

gr2 = df2.loc[(df2['Type Report'].str.contains('Cares Actual')) & (df2['item_name'].isna()), col]
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1ajFHXRIX0wXiHloH7zklrNl2kwipJLbun2I3jYCFT-0/', gr2) # manual grouping in gsheet
gr2_filled = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1ajFHXRIX0wXiHloH7zklrNl2kwipJLbun2I3jYCFT-0/').iloc[:,[0,3]].dropna(subset='item_name_group')
df2.loc[df2['Appointment ID'].isin(gr2_filled['Appointment ID']), 'item_name_group'] = gr2_filled['item_name_group'].values


#####################################################################
# process apt data for finance

df_apt_final = pd.DataFrame()
df_apt_final['Order ID'] = 'A' + df2['Appointment ID'].astype(str)
df_apt_final['Order Number'] = df2['Appointment ID']
df_apt_final['Source'] = 'Appointment'
df_apt_final['Patient ID'] = df2['Patient ID'].astype('Int64').astype(str)
df_apt_final['Booked For'] = df2['Requester']
df_apt_final['Agent ID'] = df2['agent_id']
df_apt_final['Agent Name'] = df2['agent_name']
df_apt_final['Booked By'] = df2['Patient Name']
df_apt_final['Phone Number'] = df2['Phone Number']
# df_apt_final['Phone Number'] = domo.clean_phone_number(df2['Phone Number'])
df_apt_final['Birthday'] = df2['Patient Birthday']
df_apt_final['Gender'] = df2['Patient Gender']
df_apt_final['Cluster ID'] = df2['Cluster ID']
df_apt_final['Clinic ID'] = df2['Clinic ID']
df_apt_final['Clinic Name'] = df2['Clinic Name']
df_apt_final['Doctor ID'] = df2['Doctor ID']
df_apt_final['Doctor Name'] = np.where(df2['graduate_id']!=13, df2['Doctor Name'], '')
df_apt_final['Schedule Date'] = df2['Appointment Date']
df_apt_final['Schedule Time'] = df2['Appointment Time']
df_apt_final['Note'] = df2['Reason']
df_apt_final['Nurse Name'] = np.where(df2['graduate_id']==13, df2['Doctor Name'], '')
df_apt_final['Delivery Status'] = df2['Show/No-show']
df_apt_final['Create Date'] = df2['Create Date']
df_apt_final['Mode'] = df2['Appointment Mode']
df_apt_final['Apt Status'] = df2['Status']
df_apt_final['Order Status'] = ''
df_apt_final['Lab Status'] = df2['Lab Status']
df_apt_final['Payment Method'] = df2['item_payment_method'] #

df_apt_final['Payment ID'] = df2['epaid_id']
df_apt_final['Payment Date'] = df2['original_payment_date']
df_apt_final['Payment Status'] = df2['item_payment_status'] #

df_apt_final['Shipping Total'] = np.nan
df_apt_final['Item ID'] = df2['item_id']
df_apt_final['Item Name VI'] = df2['item_name'] #
# df_apt_final['extra_item_name'] = df2['extra_item_name'] #
# df_apt_final['extra_item_price'] = df2['extra_item_price'] #
# df_apt_final['extra_item_status'] = df2['extra_item_status'] #
# df_apt_final['extra_item_payment_method'] = df2['extra_item_payment_method'] #

# df_apt_final['Item Name Group'] = df2['item_name_group']
df_apt_final['Item SKU'] = np.where(df2['item_id'].isna(), '', df2['Clinic ID'].astype('Int64').astype(str) + '_' + df2['item_id'].astype('Int64').astype(str))
df_apt_final['Item Unit Price'] = df2['item_price']

df_apt_final['Item Quantity'] = df2['item_quantity'].astype(float).astype('Int64')
df_apt_final['Item Subtotal'] = df2['item_total']
df_apt_final['Nurse Fee'] = df2['Nurse Fee'].astype('Int64')
df_apt_final['Tax Rate'] = np.nan
df_apt_final['Tax Total'] = np.nan
df_apt_final['Discount Rate'] = np.nan
df_apt_final['Discount Total'] = np.nan
df_apt_final['Order Total'] = (
    df2['original_fee'].astype(float).astype('Int64')
    + df2['extra_fee_requested'].astype('Int64')
    )

df_apt_final['COGS Per Unit'] = np.nan
df_apt_final['COGS Total (including packaging)'] = np.nan
df_apt_final['Commission Rate (%) for Docosan'] = np.nan
df_apt_final['DCS Commission Revenue (VAT excluded)'] = np.nan
df_apt_final['Reimbursement Per Unit'] = np.nan
df_apt_final['Reimbursement Total'] = np.nan
df_apt_final['Telemed Fee Per Unit'] = np.nan
df_apt_final['Telemed Fee - Total'] = np.nan
df_apt_final['Margin'] = np.nan
df_apt_final['B2B'] = df2['B2B']
df_apt_final['Item Category ID'] = ''
df_apt_final['Type Report'] = df2['Type Report']
df_apt_final['Currency'] = ''
df_apt_final['Shipping Method'] = ''
df_apt_final['Company Name'] = ''
df_apt_final['Email'] = ''
df_apt_final['Address'] = ''
df_apt_final['Platform'] = df2['Platform']



# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1toxh7WoGWurp1F0R_IEhb_8KU82twtE7EClSRQMZmu4/', df_apt_final)
