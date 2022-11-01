import numpy as np
import pandas as pd
import datetime as dt
import docosan_module as domo

df = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1wvHxPkZ8wXgplRJDbdndyJjKbS7E2ZEhDTiOAQo6umk/')


# df1 = df.dropna(subset='id').loc[pd.to_datetime(df['date_created']) < dt.datetime(2022,7,1,00,00,00),:].sort_values('date_created')
df1 = df.dropna(subset='id').sort_values('date_created')

# line_items_name_groups
conditions = [
    df1['line_items_name'].str.lower().str.contains('cương dương'),
    df1['line_items_name'].str.lower().str.contains('hormone panel for women|nội tiết tố nữ giới'),
    df1['line_items_name'].str.lower().str.contains('std'),
    df1['line_items_name'].str.lower().str.contains('hiv'),
    df1['line_items_name'].str.lower().str.contains('cov'),
    df1['line_items_name'].str.lower().str.contains('dengue|sốt xuất huyết'),
    df1['line_items_name'].str.lower().str.contains('birth'),
    ]
choices = [
    'ED',
    'Hormone Women',
    'STD',
    'HIV',
    'Covid',
    'Dengue',
    'Birth Control',
    ]
df1['line_items_name_groups'] = np.select(conditions, choices, default='Other')
df1['line_items_name_groups'].value_counts()

# threshold
conditions = [
    df1['line_items_name_groups'].str.contains('ED'),
    df1['line_items_name_groups'].str.contains('Hormone Women'),
    df1['line_items_name_groups'].str.contains('STD'),
    df1['line_items_name_groups'].str.contains('HIV'),
    df1['line_items_name_groups'].str.contains('Birth Control'),
    df1['line_items_name_groups'].str.contains('Other'),
    ]
choices = [
    30,
    90,
    90,
    60,
    60,
    90,
    ]
df1['threshold'] = np.select(conditions, choices, default=9999)

# latest order per customer & relevant columns
df1['date_created'] = pd.to_datetime(df1['date_created'])
col = ['billing_customer_id_', 'billing_first_name', 'billing_last_name', 'id', 'date_created', 'line_items_name', 'line_items_name_groups', 'threshold']
df2 = df1.loc[df1.groupby(['billing_customer_id_'])['date_created'].idxmax().values, col].sort_values('date_created').reset_index(drop=True)
df2['line_items_name_groups'].value_counts()

# days_elapsed
df2['days_elapsed'] = (dt.date.today() - df2['date_created'].dt.date).dt.days
df2['date_created'] = df2['date_created'].dt.strftime('%Y-%m-%d %H:%M:%S')

# follow_up
df2['follow_up'] = np.where((df2['days_elapsed'] - df2['threshold']) > 0, 1, 0)
df2['follow_up'].sum()
df3 = df2[df2['follow_up'] == 1]
df3.sort_values('line_items_name_groups')


###################################################################
# update

# current/live df
url = 'https://docs.google.com/spreadsheets/d/1EySH2eZEXaYlWn5y43V_pBU47LRv9ebCt8TkcwqpaX0/edit#gid=0'
df_live = domo.load_gsheet(url).dropna(subset='Phone Number').loc[:,:'Threshold']

# new rows only
df_new = pd.merge(df3, df_live, how="outer", left_on='id', right_on='Order ID', indicator=True)
df_new = df_new[df_new['_merge'] == 'left_only']

# append new rows to live df
domo.append_gsheet(url, df_new.loc[:,:'threshold'])
