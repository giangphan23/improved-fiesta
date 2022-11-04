import numpy as np
import pandas as pd
import docosan_module as domo


df = domo.woo_sql_to_df('SQL/woo_orders3.sql')
df.info()
url1 = 'https://docs.google.com/spreadsheets/d/1FJ4BIEUd9IhO5Rt4RXSdhNBrFHdbqqpBLGT51bpu89U/'
domo.update_gsheet(url1, df)

url2 = 'https://docs.google.com/spreadsheets/d/1wvHxPkZ8wXgplRJDbdndyJjKbS7E2ZEhDTiOAQo6umk/'
wc_orders_live = domo.load_gsheet(url2)

df = pd.merge(df, wc_orders_live, 'outer', left_on='Order ID', right_on='id', indicator=True).sort_values('_merge')
url3 = 'https://docs.google.com/spreadsheets/d/1uabnccfBMmqNN9VeCZ-wam9XKzkxmCFm5Cm1c4GYzak/'
domo.update_gsheet(url3, df)

##########################################################################
# remove irrelevant keys
df1 = df[~df['meta_key'].str.contains('_afl|_wp_trash_meta|_gla|_edit_lock|_order_version|_edit_last|_download_permissions_granted|trp-form-language|_aw_|_wp_|utm_|handl_|Nick Name?|legal_checbox|account_password|Nguyễn Vân Anh|_wc_facebook_for_woocommerce_|_coupon_held_keys_for_users|_transaction_id|_cart_hash|_customer_')]

# reshape long to wide
df2 = df1.drop_duplicates(subset=['ID', 'meta_key'], keep='last').pivot('ID', 'meta_key', 'meta_value').sort_index(ascending=False).reset_index()

# remove irrelevant columns
df3 = df2.loc[:,:'_shipping_state']


domo.update_gsheet('https://docs.google.com/spreadsheets/d/1FJ4BIEUd9IhO5Rt4RXSdhNBrFHdbqqpBLGT51bpu89U/edit#gid=0', df3)
