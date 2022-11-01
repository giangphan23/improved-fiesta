import numpy as np
import pandas as pd
import docosan_module as domo

sh_url = 'https://docs.google.com/spreadsheets/d/1Up9fEqDPEj7AIsPz4qG9KssoDR04PIZzGBRDF_li5LE/'
df = domo.load_gsheet(sh_url).dropna(subset='Order ID')

df1 = df.loc[df['Type Report'].str.contains('Actual'), :]
df1['Create Date'] = pd.to_datetime(df1['Create Date'], dayfirst=True)

#####################################################################
df1.set_index('Create Date', inplace=True)
col = ['Patient ID', 'Order ID', 'Doctor ID', 'Clinic ID']

df_m = df1.loc[:,col].resample("M").nunique()
df_m.columns = ['Paying Users', 'Orders', 'Active Doctors', 'Active Clinics']
df_m['Repeat Paying Users'] = df1.drop_duplicates('Patient ID')['Repeat User'].resample("M").sum()
df_m['% of Repeat Paying Users'] = df_m['Repeat Paying Users'] / df_m['Paying Users']
df_m['Repeat Orders'] = df1.drop_duplicates('Order ID')['Repeat User'].resample("M").sum()
df_m['% of Repeat Orders'] = df_m['Repeat Orders'] / df_m['Orders']

df_m['New Paying Users'] = df_m['Paying Users'] - df_m['Repeat Paying Users']
df_m['% of New Users'] = df_m['New Paying Users'] / df_m['Paying Users']
df_m['Revenue'] = df1['Order Total'].resample("M").sum()
df_m['Average User Spend'] = df_m['Revenue'] / df_m['Paying Users']
df_m['Average Order Value'] = df_m['Revenue'] / df_m['Orders']
df_m['Average Order per User'] = df_m['Orders'] / df_m['Paying Users']

df_m.index = df_m.index.to_period("M")

#####################################################################
# M1, M3 retention
df1['Ordinal Month'] = df1.index.to_period('M').astype(int)-603

m1_ls = [0,0,0]
m3_ls = [0,0,0]
for m0 in range(3, df1['Ordinal Month'].max()+1):
    df_m0 = df1[df1['Ordinal Month'] == m0]
    df_m1 = df1[df1['Ordinal Month'] == m0-1]
    df_m3 = df1[df1['Ordinal Month'] == m0-3]

    ID_list_m0 = df_m0['Patient ID'].unique().tolist()
    ID_list_m1 = df_m1['Patient ID'].unique().tolist()
    ID_list_m3 = df_m3['Patient ID'].unique().tolist()
    m3 = len(set(ID_list_m3) & set(ID_list_m0))/len(ID_list_m3)
    m1 = len(set(ID_list_m1) & set(ID_list_m0))/len(ID_list_m1)

    m1_ls.append(m1)
    m3_ls.append(m3)

df_m['M1 Retention'] = m1_ls
df_m['M3 Retention'] = m3_ls

#####################################################################


#####################################################################
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/edit#gid=0', df_m.reset_index())
