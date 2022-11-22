import numpy as np
import pandas as pd
import docosan_module as domo
import json

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


#####################################################################
# extra

# test
ls = [48044,43435,46888,47139,47820,47957,48516,48648,48673,48707,48734,2972]

#
df1_with_item = df1[df1.item_detail.notna()].sort_values('Create Date')
ls = df1_with_item['Appointment ID'].unique()

# pivot item_detail
item_df = pd.DataFrame()
for i in ls:
    # i = 48044 dict
    # i = 46888 tuple
    items = df1_with_item[df1_with_item['Appointment ID']==i]['item_detail'].tolist()[0]
    payment_methods = df1_with_item[df1_with_item['Appointment ID']==i]['payment_method'].str.split(',').tolist()[0]
    payment_status = df1_with_item[df1_with_item['Appointment ID']==i]['status'].str.split(',').tolist()[0]
    try:
        items = eval(items)
    except:
        items = None
    # print(i, type(items))

    item_ori_df = pd.DataFrame()
    item_sale_df = pd.DataFrame()
    item_extra_df = pd.DataFrame()

    if items == None:
        pass

    elif type(items) == tuple:
        for item, payment_method in zip(items, payment_methods):
            # print(item, payment_method, '\n')
            try:
                item_ori_ = pd.json_normalize(item, ['items','services'])
                item_ori_['apt_id'] = i
                item_ori_['payment_method'] = payment_method
                item_ori_['payment_status'] = payment_status
                item_ori_df = pd.concat([item_ori_df, item_ori_])
            except:
                pass
                # item_ori_df = None

            try:
                item_sale_ = pd.json_normalize(item, ['items','sale_services'])
                item_sale_['apt_id'] = i
                item_sale_['payment_method'] = payment_method
                item_sale_['payment_status'] = payment_status
                item_sale_df = pd.concat([item_sale_df, item_sale_])
            except:
                pass
                # item_sale_df = None

            try:
                item_extra_ = item['items']['services']['extra']
                item_extra_ = pd.json_normalize(item_extra_)
                item_extra_['id'] = 0
                item_extra_['apt_id'] = i
                item_extra_['payment_method'] = payment_method
                item_extra_['payment_status'] = payment_status
                item_extra_df = pd.concat([item_extra_df, item_extra_])
            except:
                pass
                # item_extra_df = None

    elif type(items) == dict:
        try:
            item_ori_df = pd.json_normalize(items, ['items','services'])
            item_ori_df['apt_id'] = i
            item_ori_df['payment_method'] = payment_method
            item_ori_df['payment_status'] = payment_status
        except:
            pass
            #            item_ori_df = None

        try:
            item_sale_df = pd.json_normalize(items, ['items','sale_services'])
            item_sale_df['apt_id'] = i
            item_sale_df['payment_method'] = payment_method
            item_sale_df['payment_status'] = payment_status
        except:
            pass
            #            item_sale_df = None

        try:
            item_extra_ = items['items']['services']['extra']
            item_extra_ = pd.json_normalize(item_extra_)
            item_extra_['id'] = 0
            item_extra_['apt_id'] = i
            item_extra_['payment_method'] = payment_method
            item_extra_df['payment_status'] = payment_status
            item_extra_df = pd.concat([item_extra_df, item_extra_])
        except:
            pass
            #            item_extra_df = None
    else:
        pass

    item_df = pd.concat([item_df, item_ori_df, item_sale_df, item_extra_df], axis=0)
item_df


item_df = item_df.add_prefix('item_')
item_df = item_df.rename(columns={'item_apt_id':'Appointment ID'})
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/18684kd96cTbeJ9ES74tmac8KjMiEpGxj3jYjPy8lAB4/edit#gid=0', item_df)


#
# df2 = df1[df1.item_detail.isna()].join(item_df.set_index('item_apt_id'), on='Appointment ID')

df1_with_item = df1_with_item.join(item_df.set_index('Appointment ID'), on='Appointment ID')
# df1_with_item.original_payment_method
# df1_with_item.extra_item_payment_method

df1_no_item = df1[df1.item_detail.isna()]
df2 = pd.concat([df1_no_item, df1_with_item])
df2
