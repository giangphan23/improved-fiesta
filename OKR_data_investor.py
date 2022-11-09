import numpy as np
import pandas as pd
import docosan_module as domo

sh_url = 'https://docs.google.com/spreadsheets/d/15zCrCorhPjt5358LHnPili2oXcn5jAVn0epy1ab-7fE/'
df = domo.load_gsheet(sh_url).dropna(subset='Order ID')

df['Create Date'] = pd.to_datetime(df['Create Date'], dayfirst=True)
df1 = df.loc[df['Type Report'].str.contains('Actual'), :]

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
df1['Ordinal Month'] = df1.index.to_period('M').astype(int)-df1.index.to_period('M').astype(int)[-1]

m1_ls = [0,0,0]
m3_ls = [0,0,0]
for m0 in range(3, df1['Ordinal Month'].max()+1):
    df_m0 = df1[df1['Ordinal Month'] == m0]
    df_m1 = df1[df1['Ordinal Month'] == m0-1]
    df_m3 = df1[df1['Ordinal Month'] == m0-3] # in [m0-3, m0-2, m0-1]]?

    ID_list_m0 = df_m0['Patient ID'].unique().tolist()
    ID_list_m1 = df_m1['Patient ID'].unique().tolist()
    ID_list_m3 = df_m3['Patient ID'].unique().tolist()
    m3 = len(set(ID_list_m3) & set(ID_list_m0))/len(ID_list_m0)
    m1 = len(set(ID_list_m1) & set(ID_list_m0))/len(ID_list_m0)

    m1_ls.append(m1)
    m3_ls.append(m3)

df_m['M1 Retention'] = m1_ls
df_m['M3 Retention'] = m3_ls

# update
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/', df_m.reset_index(), 'Investor_All')

col_final = ['Paying Users', 'New Paying Users', 'Repeat Paying Users', '% of Repeat Paying Users']
df_docosan_final = df_m.loc[:,col_final].reset_index()
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/', df_docosan_final, 'Investor_Docosan')


#####################################################################
# CARES
df_cares = df.loc[df['Type Report'].str.contains('Cares Actual'), :]
df_cares.set_index('Create Date', inplace=True)

df_cares_month = df_cares.loc[:,['Patient ID']].resample("M").nunique()
df_cares_month = df_cares_month.join(df_cares.loc[:,['Item Quantity']].resample("M").sum().astype('Int64'))
df_cares_month.columns = ['CARES Unique Customers', 'Total CARES Items Ordered']
df_cares_month['Items Per Customer'] = df_cares_month['Total CARES Items Ordered'] / df_cares_month['CARES Unique Customers']
df_cares_month['Repeat Paying Users'] = df_cares.drop_duplicates('Patient ID')['Repeat User'].resample("M").sum()
df_cares_month['Returning Customer Rate (Compared to all time)'] = df_cares_month['Repeat Paying Users'] / df_cares_month['CARES Unique Customers']


# Retention
df_cares['Ordinal Month'] = df_cares.index.to_period('M').astype(int)-df_cares.index.to_period('M').astype(int)[-1]

m1_ls = []
for m0 in range(df_cares['Ordinal Month'].max()+1):
    # list of unique user of current month
    m0_user_list = df_cares.loc[df_cares['Ordinal Month'] == m0, 'Patient ID'].unique().tolist()

    # list of unique user of previous month
    m1_user_list = df_cares.loc[df_cares['Ordinal Month'] == m0-1, 'Patient ID'].unique().tolist()

    # returning = count of common users between 2 sets / count of users in current month
    m1 = len(set(m1_user_list) & set(m0_user_list))/len(m0_user_list)

    m1_ls.append(m1)

df_cares_month['Returning Customer Rate (Compared to previous month)'] = m1_ls[-df_cares_month.shape[0]:]
df_cares_month.index = df_cares_month.index.to_period("M")

# update to sheet
col_final = ['CARES Unique Customers', 'Total CARES Items Ordered', 'Items Per Customer', 'Returning Customer Rate (Compared to previous month)', 'Returning Customer Rate (Compared to all time)']
df_cares_final = df_cares_month.loc[:,col_final].reset_index()
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/', df_cares_final, 'Investor_Cares')

#####################################################################
# BOOKINGS (same as CARES)
df_bookings = df.loc[df['Type Report'].str.contains('Appointment Actual'), :]
df_bookings.set_index('Create Date', inplace=True)
df_bookings_month = df_bookings.loc[:,['Patient ID', 'Order ID']].resample("M").nunique()
df_bookings_month.columns = ['Bookings Unique Customers', 'Total Bookings']
df_bookings_month['Bookings Per Customer'] = df_bookings_month['Total Bookings'] / df_bookings_month['Bookings Unique Customers']

df_bookings_month['Repeat Paying Users'] = df_bookings.drop_duplicates('Patient ID')['Repeat User'].resample("M").sum()
df_bookings_month['Returning Customer Rate (Compared to all time)'] = df_bookings_month['Repeat Paying Users'] / df_bookings_month['Bookings Unique Customers']

# Retention
df_bookings['Ordinal Month'] = df_bookings.index.to_period('M').astype(int)-df_bookings.index.to_period('M').astype(int)[-1]

m1_ls = []
for m0 in range(df_bookings['Ordinal Month'].max()+1):
    # list of unique user of current month
    m0_user_list = df_bookings.loc[df_bookings['Ordinal Month'] == m0, 'Patient ID'].unique().tolist()

    # list of unique user of previous month
    m1_user_list = df_bookings.loc[df_bookings['Ordinal Month'] == m0-1, 'Patient ID'].unique().tolist()

    # returning = count of common users between 2 sets / count of users in current month
    m1 = len(set(m1_user_list) & set(m0_user_list))/len(m0_user_list)

    m1_ls.append(m1)

df_bookings_month['Returning Customer Rate (Compared to previous month)'] = m1_ls[-df_bookings_month.shape[0]:]
df_bookings_month.index = df_bookings_month.index.to_period("M")


# update to sheet
col_final = ['Bookings Unique Customers', 'Total Bookings', 'Bookings Per Customer', 'Returning Customer Rate (Compared to previous month)', 'Returning Customer Rate (Compared to all time)']
df_bookings_final = df_bookings_month.loc[:,col_final].reset_index()
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1doH4nGRVWhMJwIgCZG4-yEZUApcsWY2qHwGp8N3etXc/', df_bookings_final, 'Investor_Bookings')
