import numpy as np
import pandas as pd
import docosan_module as domo
import datetime as dt
from woo_data_direct_from_DB import df_woo
from apt_reformatted import df_apt_prep


#####################################################################
# combine df
df_combined = pd.concat([df_apt_prep, df_woo])
df_combined = df_combined.sort_values('Create Date', ascending=False).reset_index(drop=True)


#####################################################################
# PROCESSING

# Repeat User
df_combined['Repeat User'] = df_combined.duplicated('Patient ID', 'last').astype(int)
df_combined['New User'] = (~df_combined.duplicated('Patient ID', 'last')).astype(int)

# Gender
df_combined['Gender'] = df_combined['Gender'].fillna('')

c1 = ~df_combined['Gender'].str.contains('Male|Female')
c2 = df_combined['Type Report'].str.contains('Cares Actual')

# df_gen1 = df_combined.loc[c1&c2, ['Patient ID', 'Booked For', 'Gender']].sort_values('Booked For')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1jre8zmBlCqWh2y-I5Rx2L8R5OA16DTiWA0tSjlIlQ3Y/edit#gid=0', df_gen1)
### input gender manually on gsheet

df_gen2 = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1jre8zmBlCqWh2y-I5Rx2L8R5OA16DTiWA0tSjlIlQ3Y/edit#gid=0').loc[:,:'Gender From Name'].dropna(subset='Booked For').drop_duplicates('Booked For')

df_gen3 = pd.merge(df_combined, df_gen2[['Booked For', 'Gender From Name']], how='left', left_on='Booked For', right_on='Booked For')

df_gen3.loc[c1&c2,'Gender'] = df_gen3.loc[c1&c2,:]['Gender From Name']
df_final = df_gen3.drop('Gender From Name', axis=1)


#####################################################################
# update sheet
sh_url = 'https://docs.google.com/spreadsheets/d/15zCrCorhPjt5358LHnPili2oXcn5jAVn0epy1ab-7fE/'
domo.update_gsheet(sh_url, df_final)

# format sheet
sh = domo.get_google_cred().open_by_url(sh_url).get_worksheet(0)
sh.format(['J', 'Q'], {'numberFormat': {'type': 'DATE', 'pattern': 'dd/mm/yyyy'}})
sh.format(['U', 'AC'], {'numberFormat': {'type': 'DATE_TIME', 'pattern': 'dd/mm/yyyy hh:mm:ss'}})
sh.format(['BN', 'BO'], {'numberFormat': {'type': 'NUMBER', 'pattern': '?#'}})
