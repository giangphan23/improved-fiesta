import numpy as np
import pandas as pd
import docosan_module as domo
from finance_data_woo_2 import df_woo_final
from finance_data_apt_2 import df_apt_final
# requiments: https://docs.google.com/spreadsheets/d/1rN4PhfMlpDGpdFFizYNZF2oSp5qhMmL2/edit#gid=1595865055


#####################################################################
# combine df
df_1 = pd.concat([df_apt_final, df_woo_final])
df_1 = df_1.sort_values('Create Date', ascending=False).reset_index(drop=True)


#####################################################################
# PROCESSING

# Repeat User
df_1['Repeat User'] = df_1.duplicated('Patient ID', 'last').astype('Int64')
df_1['New User'] = (~df_1.duplicated('Patient ID', 'last')).astype('Int64')

# Gender
df_1['Gender'] = df_1['Gender'].fillna('')

c1 = ~df_1['Gender'].str.contains('Male|Female')
c2 = df_1['Type Report'].str.contains('Cares Actual')
# df_gen1 = df_1.loc[c1&c2, ['Patient ID', 'Booked For', 'Gender']].sort_values('Booked For')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1jre8zmBlCqWh2y-I5Rx2L8R5OA16DTiWA0tSjlIlQ3Y/edit#gid=0', df_gen1)
### input gender manually in gsheet
df_gen2 = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1jre8zmBlCqWh2y-I5Rx2L8R5OA16DTiWA0tSjlIlQ3Y/edit#gid=0').loc[:,:'Gender From Name'].dropna(subset='Booked For').drop_duplicates('Booked For')
df_gen3 = pd.merge(df_1, df_gen2[['Booked For', 'Gender From Name']], how='left', left_on='Booked For', right_on='Booked For')
df_gen3.loc[c1&c2,'Gender'] = df_gen3.loc[c1&c2,:]['Gender From Name']
df_2 = df_gen3.drop('Gender From Name', axis=1)

# Agent ID
df_2['Agent ID'] = df_2['Agent ID'].replace('', np.nan)
df_2['Agent ID'] = df_2.groupby(['Agent Name'])['Agent ID'].ffill()
df_2['Agent ID'] = df_2.groupby(['Agent Name'])['Agent ID'].bfill() # for cases starting with NA


#####################################################################
# correct data type
df_3 = df_2.convert_dtypes(convert_string=False)

# string
for col in df_3.columns:
    if 'ID' in col:
        df_3[col] = np.where(df_3[col].isna(),'',df_3[col].astype(str))

# datetime
cols = ['Birthday', 'Schedule Date']
for c in cols:
    df_3[c] = pd.to_datetime(df_3[c], errors = 'coerce', yearfirst=True).dt.date

# integer
cols = ['Item Unit Price', 'Item Quantity', 'Item Subtotal']
for c in cols:
    df_3[c] = df_3[c].astype('Int64')



#####################################################################
# READY
df_combined_ready = df_3.copy()
df_combined_ready['Payment Method'].unique()



#####################################################################
# LOAD
sh_url = 'https://docs.google.com/spreadsheets/d/15zCrCorhPjt5358LHnPili2oXcn5jAVn0epy1ab-7fE/'
domo.update_gsheet(sh_url, df_combined_ready)


# format sheet
sh = domo.get_google_cred().open_by_url(sh_url).get_worksheet(0)
sh.format(['J', 'Q'], {'numberFormat': {'type': 'DATE', 'pattern': 'dd/mm/yyyy'}})
sh.format(['V', 'AC'], {'numberFormat': {'type': 'DATE_TIME', 'pattern': 'dd/mm/yyyy hh:mm:ss'}})


