import numpy as np
import pandas as pd
import docosan_module as domo

sh_url = 'https://docs.google.com/spreadsheets/d/1Up9fEqDPEj7AIsPz4qG9KssoDR04PIZzGBRDF_li5LE/'
df = domo.load_gsheet(sh_url)

df_actual = df.loc[df['Type Report'].str.contains('Actual'), ['Patient ID', 'Order ID', 'Doctor ID', 'Clinic ID', 'Create Date', 'Repeat User']]
df_actual['Create Date'] = pd.to_datetime(df_actual['Create Date'], dayfirst=True)
df_actual.set_index('Create Date', inplace=True)
df_m = df_actual.loc[:,:'Clinic ID'].resample("M").nunique()
df_m['Repeat User'] = df_actual['Repeat User'].resample("M").sum()
df_m['New User'] = df_m['Patient ID'] - df_m['Repeat User']

df_m.columns = ['Active Paying Users', 'Sessions (Transactions)', 'Active Doctors', 'Active Clinics', 'Repeat Active Paying Users', 'New Active Paying Users']

# M1, M3 retention
df_actual['Ordinal Month'] = df_actual.index.to_period('M').astype(int)-603

m1_ls = [0,0,0]
m3_ls = [0,0,0]
for m0 in range(3, df_actual['Ordinal Month'].max()+1): 
    df_m0 = df_actual[df_actual['Ordinal Month'] == m0]
    df_m1 = df_actual[df_actual['Ordinal Month'] == m0-1]
    df_m3 = df_actual[df_actual['Ordinal Month'] == m0-3]

    ID_list_m0 = df_m0['Patient ID'].unique().tolist()
    ID_list_m1 = df_m1['Patient ID'].unique().tolist()
    ID_list_m3 = df_m3['Patient ID'].unique().tolist()
    m3 = len(set(ID_list_m3) & set(ID_list_m0))/len(ID_list_m3)
    m1 = len(set(ID_list_m1) & set(ID_list_m0))/len(ID_list_m1)

    m1_ls.append(m1)
    m3_ls.append(m3)

df_m['M1 Retention'] = m1_ls
df_m['M3 Retention'] = m3_ls

domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/edit#gid=0', df_m.reset_index())
