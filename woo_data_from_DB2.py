import numpy as np
import pandas as pd
import docosan_module as domo

# post_sql = 'SELECT * FROM wp_posts;'
# wp_posts_df = domo.woo_single_SQL_query_to_df(post_sql)

# postmeta_sql = 'SELECT * FROM wp_postmeta;'
# wp_postmeta_df = domo.woo_single_SQL_query_to_df(postmeta_sql)

# item_sql = 'SELECT * FROM wp_woocommerce_order_items;'
# wp_woocommerce_order_items_df = domo.woo_single_SQL_query_to_df(item_sql)

# itemmeta_sql = 'SELECT * FROM wp_woocommerce_order_itemmeta;'
# wp_woocommerce_order_itemmeta_df = domo.woo_single_SQL_query_to_df(itemmeta_sql)

wp_posts_df.to_pickle('wp_posts_df.pkl')
wp_postmeta_df.to_pickle('wp_postmeta_df.pkl')
wp_woocommerce_order_items_df.to_pickle('wp_woocommerce_order_items_df.pkl')
wp_woocommerce_order_itemmeta_df.to_pickle('wp_woocommerce_order_itemmeta_df.pkl')

wp_posts_df = pd.read_pickle('wp_posts_df.pkl')
wp_postmeta_df = pd.read_pickle('wp_postmeta_df.pkl')

wp_woocommerce_order_items_df = pd.read_pickle('wp_woocommerce_order_items_df.pkl')
wp_woocommerce_order_itemmeta_df = pd.read_pickle('wp_woocommerce_order_itemmeta_df.pkl')

# merge posts & postmeta
post_info_df = wp_posts_df.set_index('ID').join(wp_postmeta_df.set_index('post_id'))


##########################################################################
# PREP ORDER & META

# order
order_df = post_info_df.loc[post_info_df['post_type'].str.contains('shop_order'), ['post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']].sort_index(ascending=False)

# keep relevant keys only
order_df1 = order_df[order_df['meta_key'].str.contains('^_billing_|^_shipping_|^_name_|^_order_|^_payment|^_paid')].reset_index(names='order_id')

# rename columns
order_df1.columns = ['order_id', 'created_date', 'updated_date', 'customer_note', 'status', 'meta_key', 'meta_value']

# check for duplicates
order_df1[order_df1.duplicated(['order_id', 'meta_key'], False)]

# reshape long to wide
order_df2 =
order_df1.drop_duplicates(subset=['order_id', 'meta_key'], keep='last').pivot(index=['order_id', 'created_date', 'updated_date', 'customer_note', 'status'], columns='meta_key', values='meta_value').reset_index(level=['created_date', 'updated_date', 'customer_note', 'status']).sort_index(ascending=False)




##########################################################################
# PREP ITEM META
wp_woocommerce_order_itemmeta_df1 = wp_woocommerce_order_itemmeta_df[~wp_woocommerce_order_itemmeta_df.order_item_type.str.lower().str.contains('-|₫|mặt hàng')]

wp_woocommerce_order_items_df['p_id'] = wp_woocommerce_order_items_df['order_id'].astype(str) + ' | ' + wp_woocommerce_order_items_df['order_item_name']

wp_woocommerce_order_items_df[wp_woocommerce_order_items_df.duplicated('p_id', False)]
.pivot('p_id', 'order_item_type', 'order_item_name')
wp_woocommerce_order_items_df.pivot('order_id', 'order_item_type', 'order_item_name')




wp_woocommerce_order_itemmeta_df1.pivot('order_item_id', 'order_item_type', 'order_item_name')

wp_woocommerce_order_itemmeta_df1.pivot('order_item_id', 'order_item_type', 'order_item_name')





##########################################################################
# PREP PRODUCT META
prod_df = post_info_df.loc[post_info_df['post_type'].str.contains('product'), ['post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']].sort_index(ascending=False)

# product
prod_df = wp_posts_df.loc[wp_posts_df['post_type'].str.contains('product'), ['ID', 'post_date', 'post_modified']]
prod_df.columns = ['prod_id', 'created_date', 'updated_date']

# keep relevant keys only
prodmeta_df = wp_postmeta_df[wp_postmeta_df['meta_key'].str.contains('^_billing_|^_shipping_|^_name_|^_order_|^_payment|^_paid')]
prodmeta_df.rename(columns={'post_id': 'order_id'}, inplace=True)

# reshape long to wide
ordermeta_df2 = ordermeta_df.drop('meta_id', axis=1).drop_duplicates(subset=['order_id', 'meta_key'], keep='last').pivot(index='order_id', columns='meta_key', values='meta_value').sort_index(ascending=False)






##########################################################################
# filter internal orders
ordermeta_df3 = ordermeta_df2[
    ~ordermeta_df2['_billing_first_name'].str.lower().str.contains('test').fillna(False) &
    ~ordermeta_df2['_billing_last_name'].str.lower().str.contains('test').fillna(False) &
    ~ordermeta_df2['_billing_email'].str.lower().str.contains('test').fillna(False) &
    ~ordermeta_df2['_billing_company'].str.lower().str.contains('docosan|test|abcd').fillna(False) &
    ~ordermeta_df2['_shipping_first_name'].str.lower().str.contains('test').fillna(False) &
    ~ordermeta_df2['_shipping_last_name'].str.lower().str.contains('test').fillna(False) &
    ~ordermeta_df2['_shipping_company'].str.lower().str.contains('docosan|test').fillna(False)
    ]

# clean phone number
ordermeta_df3['_billing_phone_cleaned'] = domo.clean_phone_number(ordermeta_df3['_billing_phone'])


#####################################################################
# join
ordermeta_df_final = ordermeta_df3.join(order_df1).join(items_df2).reset_index(level='order_item_id').dropna(subset='_product_id')
ordermeta_df_final.shape


# Type Report column
conditions = [
    ordermeta_df_final['status'].str.contains('completed|cod-completed|tele-pending|customer-testing|tvlk-completed|outfordelivery|lab-testing'),
    ordermeta_df_final['status'].str.contains('failed|cancelled|refunded|cod-rejected|processing|pending|on-hold|'),
    ]
choices = [
    'Cares Actual',
    'Cares Adjustment',
    ]
ordermeta_df_final['Type Report'] = np.select(conditions, choices, default='')

# replace values in payment_method
ordermeta_df_final['_payment_method'] = ordermeta_df_final['_payment_method'].replace({
    'cheque':'Bank Transfer',
    'cod': 'COD',
    'onepayvn': 'OnePay',
    'onepayus': 'OnePay',
    'other': 'Other'
}).fillna('')

# item grouping
# col = ['id', 'line_items_name']
# gr1 = ordermeta_df_final.loc[:, col].drop_duplicates(subset='line_items_name').sort_values('id')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=0', gr1) # manual grouping in gsheet
line_items_name_group = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=0').iloc[:, 1:3].dropna(subset='line_items_name')
ordermeta_df_final = pd.merge(ordermeta_df_final, line_items_name_group, how='left', left_on='_line_item_name', right_on='line_items_name')


#####################################################################
# process woo data for finance
df_woo = pd.DataFrame(index=ordermeta_df_final.index)
df_woo['Order ID'] = 'W' + ordermeta_df_final.index.astype('Int64').astype(str)
df_woo['Order Number'] = ordermeta_df_final.index.astype('Int64').astype(str)
df_woo['Source'] = 'Woo'
df_woo['Patient ID'] = ordermeta_df_final['_billing_phone_cleaned'].values
df_woo['Booked For'] = (ordermeta_df_final['_shipping_first_name'] + ' ' + ordermeta_df_final['_shipping_last_name']).str.strip()
df_woo['Agent ID'] = ''
df_woo['Agent Name'] = ''
df_woo['Booked By'] = df_woo['Booked For']
df_woo['Phone Number'] = ordermeta_df_final['_billing_phone_cleaned']
df_woo['Birthday'] = ''
df_woo['Gender'] = ''
df_woo['Clinic ID'] = ''
df_woo['Clinic Name'] = ''
df_woo['Doctor ID'] = ''
df_woo['Doctor Name'] = ''
df_woo['Schedule Date'] = ordermeta_df_final['created_date']
df_woo['Schedule Time'] = pd.to_datetime(ordermeta_df_final['created_date']).dt.strftime('%H:%M:%S')
df_woo['Note'] = ordermeta_df_final['customer_note']
# df_woo_prep['Nurse Name'] = ''
df_woo['Create Date'] = pd.to_datetime(ordermeta_df_final['created_date'])
df_woo['Mode'] = ''
df_woo['Apt Status'] = ''
df_woo['Order Status'] = ordermeta_df_final['status']
# df_woo_prep['Delivery Status'] = ''
df_woo['Lab Status'] = ''
df_woo['Payment Method'] = ordermeta_df_final['_payment_method']
# df_woo_prep['Payment ID'] = ''
df_woo['Payment Date'] = pd.to_datetime(ordermeta_df_final['_paid_date'])
# df_woo_prep['Payment Status'] = ordermeta_df_final['original_fee_status']
df_woo['Shipping Total'] = (ordermeta_df_final['_order_shipping'] + ordermeta_df_final['_order_shipping_tax'])
df_woo['Product ID'] = ordermeta_df_final['_product_id']
df_woo['Item Name vi'] = ordermeta_df_final['_line_item_name']



df_woo['Item Name Group'] = ordermeta_df_final['line_items_name_group']
# df_woo_prep['Item SKU']
df_woo['Item Unit Price'] = ordermeta_df_final['_line_subtotal'].astype('Int64')


# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1AhariGN_ISezVTDMpD-hmzJbPyA4nEp8s782mLBiWWc/edit#gid=0', ordermeta_df_final.reset_index())
##########################################################################

df_woo['Item Quantity'] = ordermeta_df_final['line_items_quantity']
df_woo['Item Total'] = ordermeta_df_final['line_items_total'].astype('Int64')
df_woo['Nurse Fee'] = 0
# df_woo_prep['Tax Rate'] = 0
df_woo['Tax Total'] = ordermeta_df_final['total_tax'].astype('Int64')
# df_woo_prep['Discount Rate']
df_woo['Discount Total'] = ordermeta_df_final['discount_total'].astype('Int64')
df_woo['Order Total'] = df_woo['Shipping Total'].fillna(0) + df_woo['Item Total'].fillna(0) + df_woo['Nurse Fee'].fillna(0) + df_woo['Tax Total'].fillna(0) - df_woo['Discount Total'].fillna(0)
# df_woo_prep['COGS Per Unit'] = 0
# df_woo_prep['COGS Total (including packaging)'] = 0
# df_woo_prep['Commission Rate (%) for Docosan'] = 0
# df_woo_prep['DCS Commission Revenue (VAT excluded)'] = 0
# df_woo_prep['Reimbursement Per Unit'] = 0
# df_woo_prep['Reimbursement Total'] = 0
# df_woo_prep['Telemed Fee Per Unit'] = 0
# df_woo_prep['Telemed Fee - Total'] = 0
# df_woo_prep['Margin'] = 0
# df_woo_prep['B2B'] = ordermeta_df_final['B2B']
# df_woo_prep['Product Category ID'] = ''
# df_woo_prep['Product Category'] = ''
df_woo['Type Report'] = ordermeta_df_final['Type Report']
df_woo['Currency'] = ordermeta_df_final['currency']
df_woo['Shipping Method'] = ordermeta_df_final['shipping_lines_method_title']
df_woo['Company Name'] = ordermeta_df_final['billing_company']
df_woo['Email'] = ordermeta_df_final['billing_email']
df_woo['Address'] = (
    ordermeta_df_final['shipping_address_1']
    + ' | ' + ordermeta_df_final['shipping_city']
    + ' | ' + ordermeta_df_final['shipping_postcode']
    + ' | ' + ordermeta_df_final['shipping_country']
    )

