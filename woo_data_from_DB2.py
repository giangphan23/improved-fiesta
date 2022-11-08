import numpy as np
import pandas as pd
import docosan_module as domo

# posts_sql = 'SELECT * FROM wp_posts;'
# wp_posts_df = domo.woo_single_SQL_query_to_df(posts_sql)

# postmeta_sql = 'SELECT * FROM wp_postmeta;'
# wp_postmeta_df = domo.woo_single_SQL_query_to_df(postmeta_sql)

# items_sql = 'SELECT * FROM wp_woocommerce_order_items;'
# wp_woocommerce_order_items_df = domo.woo_single_SQL_query_to_df(items_sql)

# itemmeta_sql = 'SELECT * FROM wp_woocommerce_order_itemmeta;'
# wp_woocommerce_order_itemmeta_df = domo.woo_single_SQL_query_to_df(itemmeta_sql)

# order_product_lookup_sql = 'SELECT * FROM wp_wc_order_product_lookup;'
# wp_wc_order_product_lookup_df = domo.woo_single_SQL_query_to_df(order_product_lookup_sql)

# wp_posts_df.to_pickle('wp_posts_df.pkl')
# wp_postmeta_df.to_pickle('wp_postmeta_df.pkl')
# wp_woocommerce_order_items_df.to_pickle('wp_woocommerce_order_items_df.pkl')
# wp_woocommerce_order_itemmeta_df.to_pickle('wp_woocommerce_order_itemmeta_df.pkl')
# wp_wc_order_product_lookup_df.to_pickle('wp_wc_order_product_lookup_df.pkl')

wp_posts_df = pd.read_pickle('wp_posts_df.pkl')
wp_postmeta_df = pd.read_pickle('wp_postmeta_df.pkl')
wp_woocommerce_order_items_df = pd.read_pickle('wp_woocommerce_order_items_df.pkl')
wp_woocommerce_order_itemmeta_df = pd.read_pickle('wp_woocommerce_order_itemmeta_df.pkl')
wp_wc_order_product_lookup_df = pd.read_pickle('wp_wc_order_product_lookup_df.pkl')

# merge posts & postmeta
post_info_df = wp_posts_df.set_index('ID').join(wp_postmeta_df.set_index('post_id'))


##########################################################################
# PREP ORDER & META

# order
order_df = post_info_df.loc[post_info_df['post_type'].str.contains('shop_order'), ['post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']].sort_index(ascending=False)

# keep relevant keys only
order_df = order_df[order_df['meta_key'].str.contains('^_billing_|^_shipping_|^_name_|^_order_|^_payment|^_paid')].reset_index(names='order_id')

# rename columns
order_df.columns = ['order_id', 'created_date', 'updated_date', 'customer_note', 'status', 'meta_key', 'meta_value']

# check for duplicates => ok to drop
order_df[order_df.duplicated(['order_id', 'meta_key'], False)]

# reshape long to wide
order_info_df = order_df.drop_duplicates(subset=['order_id', 'meta_key'], keep='last').pivot(index=['order_id', 'created_date', 'updated_date', 'customer_note', 'status'], columns='meta_key', values='meta_value').reset_index(level=['created_date', 'updated_date', 'customer_note', 'status']).sort_index(ascending=False)


##########################################################################
# PREP PRODUCT META
# product
prod_df = wp_posts_df.loc[wp_posts_df['post_type'].str.contains('^product'), ['ID', 'post_date', 'post_modified', 'post_type', 'post_title', 'post_excerpt', 'post_parent']]
prod_df.set_index('ID', inplace=True)

# product meta - keep relevant keys only
relevant_keys = '_regular_price|_sale_price|^_stock|^_tax|_wc_review_count|_wc_average_rating|^attribute_|^name_|^nurse_fee|total_sales'
prod_meta_df = wp_postmeta_df[~(wp_postmeta_df['meta_key'].str.contains('^fb_'))&(wp_postmeta_df['meta_key'].str.contains(relevant_keys))]

# join product & product meta
prod_info_df = prod_df.join(prod_meta_df.set_index('post_id')).reset_index(names='product_id')

# check for duplicates => dups in prices are prod variations
dup = prod_info_df[prod_info_df.duplicated(subset=['product_id', 'meta_key'], keep=False)]
    # if prod is variable (product_id in post_parent) => prices in child prod
    # if prod is simple (product_id NOT in post_parent) => _price = _sale_price if exists, else = _regular price
    # ACTION: drop _price in meta_key

# reshape long to wide
idx = ['product_id', 'post_date', 'post_modified', 'post_type', 'post_title', 'post_excerpt', 'post_parent']
prod_info_df = prod_info_df.pivot(index=idx, columns='meta_key', values='meta_value').reset_index(level=idx[1:]).sort_index(ascending=False)

prod_info_df.columns = ['product_created', 'product_updated', 'product_type', 'product_name', 'product_description', 'product_parent', '_regular_price', '_sale_price', '_stock', '_stock_status', '_tax_class', '_tax_status', '_wc_average_rating', '_wc_review_count', 'attribute_gioi-tinh', 'attribute_hinh-thuc-lay-mau', 'attribute_ngon-ngu-huong-dan', 'attribute_tuoi', 'name_en', 'name_vi', 'nurse_fee', 'total_sales']

order_product_lookup_df = wp_wc_order_product_lookup_df.loc[:,['order_item_id', 'product_id', 'variation_id']].set_index('product_id')
prod_info_lookup_df = order_product_lookup_df.join(prod_info_df).reset_index()

##########################################################################
# PREP ITEM & META

items_df = wp_woocommerce_order_items_df.copy().sort_values('order_id').set_index('order_item_id')
# items_df.order_item_type.unique()

# line_item data (line_item = product; should we combine same products in an order?)
items_line_item = items_df[items_df['order_item_type'].isin(['line_item'])].reset_index().set_index('order_id').drop('order_item_type', axis=1)

# shipping data
items_shipping = items_df[items_df['order_item_type'].isin(['shipping'])]
items_shipping_info = items_shipping.join(itemmeta_df_pivoted).reset_index().loc[:,['order_item_name', 'method_id', 'order_id', 'cost']]
items_shipping_info.columns = ['shipping_method_name', 'shipping_method_id', 'order_id', 'shipping_cost']
items_shipping_info.set_index('order_id', inplace=True)

# tax data
items_tax = items_df[items_df['order_item_type'].isin(['tax'])]
# NOT USED YET; CONTINUE WHEN USED

# coupon data
items_coupon = items_df[items_df['order_item_type'].isin(['coupon'])]
items_coupon_info = items_coupon.join(itemmeta_df_pivoted).reset_index().loc[:,['order_item_name', 'order_id', 'coupon_data', 'discount_amount']]
items_coupon_info.columns = ['coupon_code', 'order_id', 'coupon_data', 'coupon_discount_amount']
items_coupon_info.set_index('order_id', inplace=True)

# nurse-fee
# add nurse-fee when dev finishes limitting 1 nurse-fee line per order
items_nurse_fee = items_df[items_df['order_item_type'].isin(['nurse-fee'])]
items_nurse_fee_info = items_nurse_fee.join(itemmeta_df_pivoted).reset_index().loc[:,['order_id', '_nurse_fee_amount', '_nurse_line_total']].set_index('order_id')

# join - 1 line_item in each row
item_info_df = items_line_item.join(items_shipping_info).join(items_coupon_info)
item_info_df.info()


##########################################################################
# THE BIG JOIN
woo_df = order_info_df.join(item_info_df).join(prod_info_lookup_df.set_index('order_item_id'), on='order_item_id').reset_index()
woo_df.info()

##########################################################################
# PROCESS continued
# filter internal orders
woo_df_1 = woo_df[
    ~woo_df['_billing_first_name'].str.lower().str.contains('test').fillna(False) &
    ~woo_df['_billing_last_name'].str.lower().str.contains('test').fillna(False) &
    ~woo_df['_billing_email'].str.lower().str.contains('test').fillna(False) &
    ~woo_df['_billing_company'].str.lower().str.contains('docosan|test|abcd').fillna(False) &
    ~woo_df['_shipping_first_name'].str.lower().str.contains('test').fillna(False) &
    ~woo_df['_shipping_last_name'].str.lower().str.contains('test').fillna(False) &
    ~woo_df['_shipping_company'].str.lower().str.contains('docosan|test').fillna(False)
    ]

# clean phone number
woo_df_1['_billing_phone_cleaned'] = domo.clean_phone_number(woo_df_1['_billing_phone'])

# Type Report column
conditions = [
    woo_df_1['status'].str.contains('completed|cod-completed|tele-pending|customer-testing|tvlk-completed|outfordelivery|lab-testing'),
    woo_df_1['status'].str.contains('failed|cancelled|refunded|cod-rejected|processing|pending|on-hold|'),
    ]
choices = [
    'Cares Actual',
    'Cares Adjustment',
    ]
woo_df_1['Type Report'] = np.select(conditions, choices, default='')

# replace values in payment_method
woo_df_1['_payment_method'] = woo_df_1['_payment_method'].replace({
    'cheque':'Bank Transfer',
    'cod': 'COD',
    'onepayvn': 'OnePay',
    'onepayus': 'OnePay',
    'other': 'Other'
}).fillna('')

# item grouping
# col = ['id', 'line_items_name']
# gr1 = woo_df_1.loc[:, col].drop_duplicates(subset='line_items_name').sort_values('id')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=0', gr1) # manual grouping in gsheet
line_items_name_group = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=0').iloc[:, 1:3].dropna(subset='line_items_name')
woo_df_final = woo_df_1.join(line_items_name_group.set_index('line_items_name'), on='name_vi')

#####################################################################
# process woo data for finance
df_woo = pd.DataFrame(index=woo_df_final.index)
df_woo['Order ID'] = 'W' + woo_df_final.index.astype('Int64').astype(str)
df_woo['Order Number'] = woo_df_final.index.astype('Int64').astype(str)
df_woo['Source'] = 'Woo'
df_woo['Patient ID'] = woo_df_final['_billing_phone_cleaned'].values
df_woo['Booked For'] = (woo_df_final['_shipping_first_name'] + ' ' + woo_df_final['_shipping_last_name']).str.strip()
df_woo['Agent ID'] = ''
df_woo['Agent Name'] = ''
df_woo['Booked By'] = df_woo['Booked For']
df_woo['Phone Number'] = woo_df_final['_billing_phone_cleaned']
df_woo['Birthday'] = ''
df_woo['Gender'] = ''
df_woo['Clinic ID'] = ''
df_woo['Clinic Name'] = ''
df_woo['Doctor ID'] = ''
df_woo['Doctor Name'] = ''
df_woo['Schedule Date'] = woo_df_final['created_date']
df_woo['Schedule Time'] = pd.to_datetime(woo_df_final['created_date']).dt.strftime('%H:%M:%S')
df_woo['Note'] = woo_df_final['customer_note']
# df_woo_prep['Nurse Name'] = ''
df_woo['Create Date'] = pd.to_datetime(woo_df_final['created_date'])
df_woo['Mode'] = ''
df_woo['Apt Status'] = ''
df_woo['Order Status'] = woo_df_final['status']
# df_woo_prep['Delivery Status'] = ''
df_woo['Lab Status'] = ''
df_woo['Payment Method'] = woo_df_final['_payment_method']
# df_woo_prep['Payment ID'] = ''
df_woo['Payment Date'] = pd.to_datetime(woo_df_final['_paid_date'])
# df_woo_prep['Payment Status'] = woo_df_final['original_fee_status']
df_woo['Shipping Total'] = (woo_df_final['_order_shipping'] + woo_df_final['_order_shipping_tax'])
df_woo['Product ID'] = woo_df_final['product_id']
df_woo['Item Name VI'] = woo_df_final['name_vi']
df_woo['Item Name EN'] = woo_df_final['name_en']
df_woo['Item Name Group'] = woo_df_final['line_items_name_group']
# df_woo_prep['Item SKU']
df_woo['Item Unit Price'] = woo_df_final['_line_subtotal'].astype('Int64')


# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1AhariGN_ISezVTDMpD-hmzJbPyA4nEp8s782mLBiWWc/edit#gid=0', woo_df_final)
##########################################################################

df_woo['Item Quantity'] = woo_df_final['line_items_quantity']
df_woo['Item Total'] = woo_df_final['line_items_total'].astype('Int64')
df_woo['Nurse Fee'] = 0
# df_woo_prep['Tax Rate'] = 0
df_woo['Tax Total'] = woo_df_final['total_tax'].astype('Int64')
# df_woo_prep['Discount Rate']
df_woo['Discount Total'] = woo_df_final['discount_total'].astype('Int64')
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
# df_woo_prep['B2B'] = woo_df_final['B2B']
# df_woo_prep['Product Category ID'] = ''
# df_woo_prep['Product Category'] = ''
df_woo['Type Report'] = woo_df_final['Type Report']
df_woo['Currency'] = woo_df_final['currency']
df_woo['Shipping Method'] = woo_df_final['shipping_lines_method_title']
df_woo['Company Name'] = woo_df_final['billing_company']
df_woo['Email'] = woo_df_final['billing_email']
df_woo['Address'] = (
    woo_df_final['shipping_address_1']
    + ' | ' + woo_df_final['shipping_city']
    + ' | ' + woo_df_final['shipping_postcode']
    + ' | ' + woo_df_final['shipping_country']
    )

