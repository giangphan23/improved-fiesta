import numpy as np
import pandas as pd
import docosan_module as domo


##########################################################################
##########################################################################
# EXTRACT

posts_sql = 'SELECT * FROM wp_posts;'
wp_posts_df = domo.woo_single_SQL_query_to_df(posts_sql)

postmeta_sql = 'SELECT * FROM wp_postmeta;'
wp_postmeta_df = domo.woo_single_SQL_query_to_df(postmeta_sql)

items_sql = 'SELECT * FROM wp_woocommerce_order_items;'
wp_woocommerce_order_items_df = domo.woo_single_SQL_query_to_df(items_sql)

itemmeta_sql = 'SELECT * FROM wp_woocommerce_order_itemmeta;'
wp_woocommerce_order_itemmeta_df = domo.woo_single_SQL_query_to_df(itemmeta_sql)

order_product_lookup_sql = 'SELECT * FROM wp_wc_order_product_lookup;'
wp_wc_order_product_lookup_df = domo.woo_single_SQL_query_to_df(order_product_lookup_sql)

wp_users_sql = 'SELECT * FROM wp_users;'
wp_users_df = domo.woo_single_SQL_query_to_df(wp_users_sql)

wp_terms_sql = 'SELECT * FROM wp_terms;'
wp_terms_df = domo.woo_single_SQL_query_to_df(wp_terms_sql)

wp_termmeta_sql = 'SELECT * FROM wp_termmeta;'
wp_termmeta_df = domo.woo_single_SQL_query_to_df(wp_termmeta_sql)

wp_term_relationships_sql = 'SELECT * FROM wp_term_relationships;'
wp_term_relationships_df = domo.woo_single_SQL_query_to_df(wp_term_relationships_sql)

wp_term_taxonomy_sql = 'SELECT * FROM wp_term_taxonomy;'
wp_term_taxonomy_df = domo.woo_single_SQL_query_to_df(wp_term_taxonomy_sql)



##########################################################################
##########################################################################
# SAVE RAW

wp_posts_df.to_pickle('wp_posts_df.pkl')
wp_postmeta_df.to_pickle('wp_postmeta_df.pkl')
wp_woocommerce_order_items_df.to_pickle('wp_woocommerce_order_items_df.pkl')
wp_woocommerce_order_itemmeta_df.to_pickle('wp_woocommerce_order_itemmeta_df.pkl')
wp_wc_order_product_lookup_df.to_pickle('wp_wc_order_product_lookup_df.pkl')
wp_users_df.to_pickle('wp_users_df.pkl')
wp_terms_df.to_pickle('wp_terms_df.pkl')
wp_termmeta_df.to_pickle('wp_termmeta_df.pkl')
wp_term_relationships_df.to_pickle('wp_term_relationships_df.pkl')
wp_term_taxonomy_df.to_pickle('wp_term_taxonomy_df.pkl')


# wp_posts_df = pd.read_pickle('wp_posts_df.pkl')
# wp_postmeta_df = pd.read_pickle('wp_postmeta_df.pkl')
# wp_woocommerce_order_items_df = pd.read_pickle('wp_woocommerce_order_items_df.pkl')
# wp_woocommerce_order_itemmeta_df = pd.read_pickle('wp_woocommerce_order_itemmeta_df.pkl')
# wp_wc_order_product_lookup_df = pd.read_pickle('wp_wc_order_product_lookup_df.pkl')
# wp_users_df = pd.read_pickle('wp_users_df.pkl')
# wp_terms_df = pd.read_pickle('wp_terms_df.pkl')
# wp_termmeta_df = pd.read_pickle('wp_termmeta_df.pkl')
# wp_term_relationships_df = pd.read_pickle('wp_term_relationships_df.pkl')
# wp_term_taxonomy_df = pd.read_pickle('wp_term_taxonomy_df.pkl')



##########################################################################
##########################################################################
# TRANSFORM

# merge posts & postmeta
post_info_df = wp_posts_df.set_index('ID').join(wp_postmeta_df.set_index('post_id'))



##########################################################################
# PREP ORDER

# order
order_df = post_info_df.loc[post_info_df['post_type'].str.contains('shop_order'), ['post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']].sort_index(ascending=False)

# keep relevant keys only
order_df = order_df[order_df['meta_key'].str.contains('^_billing_|^_shipping_|^_name_|^_order_|^_payment|^_paid|_customer_user|^_refund')].reset_index(names='order_id')

# rename columns
order_df.columns = ['order_id', 'created_at', 'updated_at', 'customer_note', 'status', 'meta_key', 'meta_value']

# check for duplicates => ok to drop
order_df[order_df.duplicated(['order_id', 'meta_key'], False)]

# reshape long to wide
order_info_df = order_df.drop_duplicates(subset=['order_id', 'meta_key'], keep='last').pivot(index=['order_id', 'created_at', 'updated_at', 'customer_note', 'status'], columns='meta_key', values='meta_value').reset_index(level=['created_at', 'updated_at', 'customer_note', 'status']).sort_index(ascending=False)

# _customer_user data
wp_users_df.ID = wp_users_df.ID.astype(str)
wp_users_df1 = wp_users_df.set_index('ID')[['user_email', 'display_name']]
wp_users_df1.columns = ['user_email', 'user_display_name']
order_info_df1 = order_info_df.join(wp_users_df1, '_customer_user')



##########################################################################
# PREP PRODUCT
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

# reshape long to wide (after dropping NA in meta_key caused by AUTO-DRAFT products)
idx = ['product_id', 'post_date', 'post_modified', 'post_type', 'post_title', 'post_excerpt', 'post_parent']
prod_info_df = prod_info_df.dropna(subset='meta_key').pivot(index=idx, columns='meta_key', values='meta_value').reset_index(level=idx[1:]).sort_index(ascending=False)

# rename
prod_info_df.columns = ['product_created', 'product_updated', 'product_type', 'product_name', 'product_description', 'product_parent', '_regular_price', '_sale_price', '_stock', '_stock_status', '_tax_class', '_tax_status', '_wc_average_rating', '_wc_review_count', 'attribute_gioi-tinh', 'attribute_hinh-thuc-lay-mau', 'attribute_ngon-ngu-huong-dan', 'attribute_tuoi', 'name_en', 'name_vi', 'nurse_fee', 'total_sales']

#
order_product_lookup_df = wp_wc_order_product_lookup_df.loc[:,['order_item_id', 'product_id', 'variation_id']].set_index('product_id')
prod_var_info_df = order_product_lookup_df.join(prod_info_df).reset_index()

# product category
prod_cate_df = wp_term_relationships_df.join(wp_term_taxonomy_df.set_index('term_taxonomy_id'), on='term_taxonomy_id').join(wp_terms_df.set_index('term_id'), on='term_id')

col = ['object_id', 'term_taxonomy_id', 'description', 'parent', 'name', 'slug']
prod_cate_df = prod_cate_df.loc[prod_cate_df.taxonomy.str.contains('product_cat'), col]
prod_cate_df.columns = ['product_id', 'category_id', 'category_description', 'category_parent', 'category_name', 'category_slug']

# combine product multiple-categories
prod_cate_df.set_index('product_id', inplace=True)
prod_cate_df2 = pd.DataFrame(index=prod_cate_df.index.drop_duplicates())
for c in prod_cate_df:
    col = prod_cate_df[c].astype(str)
    prod_cate_df2 = prod_cate_df2.join(col.groupby('product_id').apply('|'.join))
prod_cate_df2.category_parent = prod_cate_df2.category_parent.replace('0|\\|| ', '', regex=True)

prod_final_df = prod_var_info_df.join(prod_cate_df2, on='product_id')
prod_final_df['category_slug'] = prod_final_df['category_slug'].str.replace('-', ' ').str.title()



##########################################################################
# PREP ITEM

# items
items_df = wp_woocommerce_order_items_df.copy().sort_values('order_id').set_index('order_item_id')
# items_df.order_item_type.unique()

# itemmeta
itemmeta_df = wp_woocommerce_order_itemmeta_df[~wp_woocommerce_order_itemmeta_df.meta_key.str.lower().str.contains('-|₫|mặt hàng')]

# check for duplicates => ok to drop
itemmeta_df[itemmeta_df.duplicated(['order_item_id', 'meta_key'], keep=False)]
itemmeta_df = itemmeta_df.drop_duplicates(['order_item_id', 'meta_key'])

# pivot item meta
itemmeta_df_pivoted = itemmeta_df.pivot(index='order_item_id', columns='meta_key', values='meta_value')

# line_item data (line_item = product; should we combine same products in an order?)
items_line_item = items_df[items_df['order_item_type'].isin(['line_item'])].reset_index().set_index('order_id').drop('order_item_type', axis=1)
items_line_item1 = items_line_item.join(itemmeta_df_pivoted, 'order_item_id').loc[:,['order_item_id', 'order_item_name', '_line_total', '_line_subtotal', '_qty', 'total_tax']]

# shipping data
items_shipping = items_df[items_df['order_item_type'].isin(['shipping'])]
items_shipping_info = items_shipping.join(itemmeta_df_pivoted).reset_index().loc[:,['order_item_name', 'method_id', 'order_id', 'cost']]
items_shipping_info.columns = ['shipping_method_name', 'shipping_method_id', 'order_id', 'shipping_cost']
items_shipping_info.set_index('order_id', inplace=True)

# tax data
items_tax = items_df[items_df['order_item_type'].isin(['tax'])] # NOT USED YET

# coupon data
items_coupon = items_df[items_df['order_item_type'].isin(['coupon'])]
items_coupon_info = items_coupon.join(itemmeta_df_pivoted).reset_index().loc[:,['order_item_name', 'order_id']]
items_coupon_info.columns = ['coupon_code', 'order_id']
items_coupon_info.set_index('order_id', inplace=True)

# nurse-fee
# add nurse-fee when dev finishes limitting 1 nurse-fee line per order
items_nurse_fee = items_df[items_df['order_item_type'].isin(['nurse-fee'])]
items_nurse_fee_info = items_nurse_fee.join(itemmeta_df_pivoted).reset_index().loc[:,['order_id', '_nurse_fee_amount', '_nurse_line_total']].set_index('order_id')

# join - 1 line_item per row
item_info_df = items_line_item1.join(items_shipping_info).join(items_coupon_info)
# item_info_df.info()



##########################################################################
# PREP COUPON

# coupon
coupon_df = post_info_df.loc[post_info_df['post_type'].str.contains('shop_coupon'), ['post_title', 'post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']].sort_index(ascending=False)

# keep relevant keys only
coupon_df = coupon_df[~coupon_df.meta_key.str.contains('^_edit|^_wp_|^_maybe_used_by_')].reset_index(names='coupon_id')

# rename columns
coupon_df.columns = ['coupon_id', 'coupon_code', 'coupon_created_at', 'coupon_updated_at', 'coupon_description', 'coupon_status', 'meta_key', 'meta_value']

# check for duplicates => ok to drop
dup = coupon_df[coupon_df.duplicated(['coupon_id', 'meta_key'], False)]

# reshape long to wide
coupon_info_df = coupon_df.drop_duplicates(subset=['coupon_id', 'meta_key'], keep=False).pivot(index=['coupon_id', 'coupon_code', 'coupon_created_at', 'coupon_updated_at', 'coupon_description', 'coupon_status'], columns='meta_key', values='meta_value').reset_index(level=['coupon_code', 'coupon_created_at', 'coupon_updated_at', 'coupon_description', 'coupon_status']).sort_index(ascending=False)

# filter - include publish only
coupon_info_df = coupon_info_df[coupon_info_df.coupon_status.str.contains('publish')]
coupon_info_df['coupon_rate'] = np.where(coupon_info_df['discount_type'].str.contains('percent'), coupon_info_df['coupon_amount'], 0).astype(int)/100
coupon_info_df['coupon_amount'] = np.where(coupon_info_df['discount_type'].str.contains('fixed_cart'), coupon_info_df['coupon_amount'], 0).astype(int)



##########################################################################
# THE BIG JOIN
woo_df = order_info_df1.join(item_info_df).join(prod_final_df.set_index('order_item_id'), on='order_item_id').join(coupon_info_df.set_index('coupon_code'), on='coupon_code').reset_index()



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
    woo_df_1['status'].str.contains('failed|cancelled|refunded|cod-rejected|processing|pending|on-hold'),
    ]
choices = [
    'Cares Actual',
    'Cares Adjustment',
    ]
woo_df_1['Type Report'] = np.select(conditions, choices, default='')

# Delivery Status
conditions = [
    woo_df_1['status'].str.contains('outfordelivery|processing'),
    woo_df_1['status'].str.contains('completed|cod-completed|tvlk-completed|tele-pending|customer-testing'),
    woo_df_1['status'].str.contains('failed|cancelled|refunded|on-hold'),
    ]
choices = [
    'Pending',
    'Show',
    '',
    ]
woo_df_1['Delivery Status'] = np.select(conditions, choices, default='')

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
woo_df_final = woo_df_1.join(line_items_name_group.set_index('line_items_name'), on='name_vi').set_index('order_id').sort_index(ascending=False)

# SKU
woo_df_final['item_sku'] = woo_df_final['category_id'].astype(str) + '_' + woo_df_final['product_id'].astype('Int64').astype(str)
woo_df_final['item_sku'] = np.where(woo_df_final['item_sku'].str.contains('163'), '684_' + woo_df_final['item_sku'], '744_' + woo_df_final['item_sku'])
woo_df_final['item_sku'].replace('744_nan_.*', '', regex=True, inplace=True)


#####################################################################
# process woo data for finance

df_woo_final = pd.DataFrame(index=woo_df_final.index)
df_woo_final['Order ID'] = 'W' + woo_df_final.index.astype('Int64').astype(str)
df_woo_final['Order Number'] = woo_df_final.index.astype('Int64').astype(str)
df_woo_final['Source'] = 'Woo'
df_woo_final['Patient ID'] = woo_df_final['_billing_phone_cleaned'].values
df_woo_final['Booked For'] = (woo_df_final['_shipping_first_name'] + ' ' + woo_df_final['_shipping_last_name']).str.strip()
df_woo_final['Agent ID'] = ''
df_woo_final['Agent Name'] = woo_df_final['user_display_name'].fillna('')
df_woo_final['Booked By'] = df_woo_final['Booked For']
df_woo_final['Phone Number'] = woo_df_final['_billing_phone_cleaned']
# df_woo_final['Birthday'] = ''
# df_woo_final['Gender'] = ''
# df_woo_final['Clinic ID'] = ''
# df_woo_final['Clinic Name'] = ''
# df_woo_final['Doctor ID'] = ''
# df_woo_final['Doctor Name'] = ''
df_woo_final['Schedule Date'] = woo_df_final['created_at']
df_woo_final['Schedule Time'] = pd.to_datetime(woo_df_final['created_at']).dt.strftime('%H:%M:%S')
df_woo_final['Note'] = woo_df_final['customer_note']
# df_woo_final['Nurse Name'] = ''
df_woo_final['Create Date'] = pd.to_datetime(woo_df_final['created_at'])
# df_woo_final['Mode'] = ''
# df_woo_final['Apt Status'] = ''
df_woo_final['Order Status'] = woo_df_final['status']
df_woo_final['Delivery Status'] = woo_df_final['Delivery Status']
# df_woo_final['Lab Status'] = ''
df_woo_final['Payment Method'] = woo_df_final['_payment_method']
df_woo_final['Payment ID'] = woo_df_final.index.astype('Int64').astype(str)
df_woo_final['Payment Date'] = pd.to_datetime(woo_df_final['_paid_date'])
# df_woo_final['Payment Status'] = woo_df_final['original_fee_status']
df_woo_final['Item ID'] = woo_df_final['product_id']
df_woo_final['Item Name VI'] = woo_df_final['name_vi']
df_woo_final['Item Name EN'] = np.where(woo_df.name_en.isna(), woo_df.order_item_name, woo_df.name_en).__len__()
df_woo_final['Item SKU'] = woo_df_final['item_sku']
df_woo_final['Item Name Group'] = woo_df_final['line_items_name_group']
df_woo_final['Item Quantity'] = woo_df_final['_qty'].astype('Int64')
df_woo_final['Item Subtotal'] = woo_df_final['_line_subtotal'].astype(float).astype('Int64')
df_woo_final['Item Unit Price'] = (df_woo_final['Item Subtotal'] / df_woo_final['Item Quantity']).astype('Int64')
df_woo_final['Shipping Total'] = woo_df_final['_order_shipping'].astype('Int64') + woo_df_final['_order_shipping_tax'].astype('Int64')
df_woo_final['Nurse Fee'] = woo_df_final['nurse_fee'].astype('Int64')
df_woo_final['Tax Total'] = woo_df_final['total_tax'].astype('Int64')
# df_woo_final['Tax Rate'] = 0
df_woo_final['Discount Rate'] = woo_df_final['coupon_rate']
df_woo_final['Discount Fixed Amount'] = woo_df_final['coupon_amount']
df_woo_final['Discount Total'] = df_woo_final['Discount Rate'] * df_woo_final['Item Subtotal'] + df_woo_final['Discount Fixed Amount']
df_woo_final['Refund Amount'] = woo_df_final['_refund_amount'].astype('Int64')
df_woo_final['Refund Reason'] = woo_df_final['_refund_reason']
df_woo_final['Order Total'] = (
      df_woo_final['Shipping Total'].fillna(0)
    + df_woo_final['Item Subtotal'].fillna(0)
    + df_woo_final['Nurse Fee'].fillna(0)
    + df_woo_final['Tax Total'].fillna(0)
    + df_woo_final['Refund Amount'].fillna(0)
    - df_woo_final['Discount Total'].fillna(0)
    )
# df_woo_final['COGS Per Unit'] = 0
# df_woo_final['COGS Total (including packaging)'] = 0
# df_woo_final['Commission Rate (%) for Docosan'] = 0
# df_woo_final['DCS Commission Revenue (VAT excluded)'] = 0
# df_woo_final['Reimbursement Per Unit'] = 0
# df_woo_final['Reimbursement Total'] = 0
# df_woo_final['Telemed Fee Per Unit'] = 0
# df_woo_final['Telemed Fee - Total'] = 0
# df_woo_final['Margin'] = 0
# df_woo_final['B2B'] = woo_df_final['B2B']
df_woo_final['Item Category ID'] = woo_df_final['category_id']
df_woo_final['Item Category VI'] = woo_df_final['category_name']
df_woo_final['Item Category EN'] = woo_df_final['category_slug']
df_woo_final['Type Report'] = woo_df_final['Type Report']
df_woo_final['Currency'] = woo_df_final['_order_currency']
df_woo_final['Shipping Method'] = woo_df_final['shipping_method_name']
df_woo_final['Company Name'] = woo_df_final['_billing_company']
df_woo_final['Email'] = woo_df_final['_billing_email']
df_woo_final['Address'] = (
    woo_df_final['_shipping_address_1']
    + ' | ' + woo_df_final['_shipping_city']
    + ' | ' + woo_df_final['_shipping_postcode']
    + ' | ' + woo_df_final['_shipping_country']
    )



##########################################################################
##########################################################################
# LOAD
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1AhariGN_ISezVTDMpD-hmzJbPyA4nEp8s782mLBiWWc/edit#gid=0', df_woo_final)



##########################################################################
# HOW TO DEAL WITH VARIABLE PRODUCTS?

prod_info_df[prod_info_df.product_type.str.contains('vari')]
prod_info_df[prod_info_df.product_type.str.contains('vari')].product_parent.unique()
prod_info_df[prod_info_df.product_type.str.contains('vari')].product_name.unique()

prod_final_df[prod_final_df.variation_id!=0]




prod_final_df[prod_final_df._regular_price.isna()].product_id.unique()
prod_final_df[prod_final_df.product_id==4033]
prod_final_df[prod_final_df.product_type.str.contains('vari').fillna(False)].product_parent.unique()



order_product_lookup_df.loc[765,'variation_id'].unique()
order_product_lookup_df.variation_id.unique()

# woo_df = order_info_df1.join(item_info_df).join(prod_final_df.set_index('order_item_id'), on='order_item_id').join(coupon_info_df.set_index('coupon_code'), on='coupon_code').reset_index()

woo_df.set_index('order_id').loc[5901,'order_item_id':'product_ids']
order_info_df1.loc[5901,:]
item_info_df.loc[5901,:]
prod_final_df.set_index('order_item_id').loc[3418,:]


arr = woo_df.loc[woo_df.name_en.isna(), 'order_item_id'].dropna().astype(int)

# order_item_ids which exist in prod_final_df
for id in arr:
    if id in prod_final_df.order_item_id.values: print(id)

prod_final_df[prod_final_df.order_item_id==3603].T





domo.update_gsheet('https://docs.google.com/spreadsheets/d/1duyqi5lHPE9hOfJcdyRk-_0ZYZL4XjXkINEWz922cWQ/edit#gid=0', woo_df[woo_df.name_en.isna()])
