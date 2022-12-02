import numpy as np
import pandas as pd

import docosan_module as domo


##############################################################################
# EXTRACT
##############################################################################


wp_tables = [
    'wp_posts',
    'wp_postmeta',
    'wp_comments',
    'wp_wc_order_items',
    'wp_wc_order_itemmeta',
    'wp_wc_order_product_lookup',
    'wp_users',
    'wp_terms',
    'wp_termmeta',
    'wp_term_relationships',
    'wp_term_taxonomy',
    'wp_term_relationships',
    'wp_term_taxonomy',
    ]

extract_list = []
for q in wp_tables:
    q = 'SELECT * FROM ' + q + ';'
    extract_list.append(domo.extract_woo(q))

# Save raw data
for df, name in zip(extract_list, wp_tables):
    df.to_pickle('pickles/' + name + '.pkl')

# Unpack to df
(   wp_posts,
    wp_postmeta,
    wp_comments,
    wp_wc_order_items,
    wp_wc_order_itemmeta,
    wp_wc_order_product_lookup,
    wp_users,
    wp_terms,
    wp_termmeta,
    wp_term_relationships,
    wp_term_taxonomy,
    wp_term_relationships,
    wp_term_taxonomy,
) = tuple(extract_list)


##############################################################################
# TRANSFORM
##############################################################################
# Merge posts & postmeta into post_info containing order, product & coupon info
post_info_df = wp_posts.set_index('ID').join(wp_postmeta.set_index('post_id'))


##############################################################################
# Processing order info


# filter order from all posts
order_df = post_info_df.loc[
    post_info_df['post_type'].str.contains('shop_order'),
    ['post_date', 'post_modified', 'post_excerpt', 'post_status', 'meta_key', 'meta_value']
    ].sort_index(ascending=False)


# include relevant keys only
order_relevant_keys = '''^_billing_|^_shipping_|^_name_|^_order_|^_payment|
    ^_paid|_customer_user|^_refund'''
order_df = order_df[order_df['meta_key'].str.contains(order_relevant_keys)].\
    reset_index(names='order_id')


# rename columns to more meaningful names
order_df.columns = ['order_id', 'created_at', 'updated_at', 'customer_note',
    'status', 'meta_key', 'meta_value']


# check for duplicates => ok to drop
order_df[order_df.duplicated(['order_id', 'meta_key'], False)]


# pivot df (reshape long to wide)
order_df = order_df.\
    drop_duplicates(subset=['order_id', 'meta_key'], keep='last').\
    pivot(
        index=['order_id', 'created_at', 'updated_at', 'customer_note',
            'status'],
        columns='meta_key',
        values='meta_value').\
    reset_index(level=['created_at', 'updated_at', 'customer_note', 'status']).\
    sort_index(ascending=False)


# process user data
wp_users.ID = wp_users.ID.astype(str)
wp_users = wp_users.set_index('ID')[['user_email', 'display_name']]
wp_users.columns = ['user_email', 'user_display_name']


# process order note
note_cols = ['comment_post_ID', 'comment_author', 'comment_author_email', \
    'comment_date', 'comment_content']
order_note = wp_comments.loc[wp_comments['comment_type'].\
    str.contains('order_note'), note_cols]
order_note = order_note.rename(columns={'comment_post_ID':'order_id'})
order_note = order_note[order_note.comment_author_email.
    str.contains('@docosan.com')].\
    groupby('order_id').first() # keep info of staff who interacts first


# ready
order_ready = order_df.join(wp_users, '_customer_user').join(order_note)


##############################################################################
# Processing product info


# filter product from all posts
prod_relevant_cols = ['ID', 'post_date', 'post_modified', 'post_type', 'post_title', 'post_excerpt', 'post_parent']
prod_df = wp_posts.loc[
    wp_posts['post_type'].str.contains('^product'),
    prod_relevant_cols].set_index('ID')


# product meta - keep relevant keys only
prod_relevant_keys = '''_regular_price|_sale_price|^_stock|^_tax|
    _wc_review_count|_wc_average_rating|^attribute_|^name_|total_sales'''
prod_meta_df = wp_postmeta[
    ~(wp_postmeta['meta_key'].str.contains('^fb_'))
    & (wp_postmeta['meta_key'].str.contains(prod_relevant_keys))]


# join product & product meta
prod_df = prod_df.join(prod_meta_df.set_index('post_id')).\
    reset_index(names='product_id')


# check for duplicates => dups in prices are prod variations
prod_df[prod_df.duplicated(subset=['product_id', 'meta_key'], keep=False)]
    # if prod is variable (product_id in post_parent) => prices in child prod
    # if prod is simple (product_id NOT in post_parent) => _price = _sale_price if exists, else = _regular price
    # ACTION: drop _price in meta_key


# reshape long to wide (after dropping NA in meta_key caused by AUTO-DRAFT products)
idx = ['product_id', 'post_date', 'post_modified', 'post_type', 'post_title',
    'post_excerpt', 'post_parent']
prod_df = prod_df.dropna(subset='meta_key').\
    pivot(index=idx, columns='meta_key', values='meta_value').\
    reset_index(level=idx[1:]).\
    sort_index(ascending=False)


# rename
prod_df.columns = ['product_created', 'product_updated', 'product_type',
    'product_name', 'product_description', 'product_parent', '_regular_price',
    '_sale_price', '_stock', '_stock_status', '_tax_class', '_tax_status',
    '_wc_average_rating', 'attribute_gioi-tinh', 'attribute_hinh-thuc-lay-mau',
    'attribute_ngon-ngu-huong-dan', 'attribute_tuoi', 'name_en', 'name_vi',
    'total_sales']


# product variation
prod_var_lookup = wp_wc_order_product_lookup[['order_item_id', 'product_id', 'variation_id']].set_index('product_id')
prod_var = prod_var_lookup.join(prod_df).reset_index()

# product category
prod_cate = wp_term_relationships.\
    join(wp_term_taxonomy.set_index('term_taxonomy_id'), on='term_taxonomy_id').\
    join(wp_terms.set_index('term_id'), on='term_id')
prod_cate_col = ['object_id', 'term_taxonomy_id', 'description', 'parent',
    'name', 'slug']
prod_cate = prod_cate.loc[prod_cate.taxonomy.str.contains('product_cat'),
    prod_cate_col]
prod_cate.columns = ['product_id', 'category_id', 'category_description',
    'category_parent', 'category_name', 'category_slug']


# concatenate product multi-categories
prod_cate.set_index('product_id', inplace=True)
prod_multi_cate = pd.DataFrame(index=prod_cate.index.drop_duplicates())
for c in prod_cate:
    col = prod_cate[c].astype(str)
    prod_multi_cate = prod_multi_cate.join(col.groupby('product_id').
        apply('|'.join))
prod_multi_cate.category_parent = prod_multi_cate.category_parent.replace('0|\\|| ', '', regex=True)


# ready
prod_ready = prod_var.join(prod_multi_cate, on='product_id')
prod_ready['category_slug'] = prod_ready['category_slug'].str.replace('-', ' ').str.title()



##############################################################################
# Processing item info

# items
items_df = wp_wc_order_items.copy().sort_values('order_id').set_index('order_item_id')

# itemmeta
itemmeta_df = wp_wc_order_itemmeta[~wp_wc_order_itemmeta.meta_key.str.lower().str.contains('-|₫|mặt hàng')]

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
items_nurse_fee = items_df[items_df['order_item_type'].isin(['nurse-fee'])]
items_nurse_fee_info = items_nurse_fee.join(itemmeta_df_pivoted).reset_index().loc[:,['order_id', 'order_item_name', '_nurse_fee_amount']].set_index('order_id')
items_nurse_fee_info = items_nurse_fee_info.rename(columns={'order_item_name':'nurse_name', '_nurse_fee_amount':'nurse_fee'})


# join - 1 line_item per row
item_info_df = items_line_item1.join(items_shipping_info).join(items_coupon_info).join(items_nurse_fee_info)




##############################################################################
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



##############################################################################
# THE BIG JOIN
woo_df = order_ready.join(item_info_df).join(prod_ready.set_index('order_item_id'), on='order_item_id').join(coupon_info_df.set_index('coupon_code'), on='coupon_code').reset_index()



##############################################################################
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
    woo_df_1['status'].str.contains('auto-draft|trash'),
    ]
choices = [
    'Cares Actual',
    'Cares Adjustment',
    'None Adjustment',
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

# SKU
woo_df_1['item_sku'] = woo_df_1['category_id'].astype(str) + '_' + woo_df_1['product_id'].astype('Int64').astype(str)
woo_df_1['item_sku'] = np.where(woo_df_1['item_sku'].str.contains('163'), '684_' + woo_df_1['item_sku'], '744_' + woo_df_1['item_sku'])
woo_df_1['item_sku'].replace('744_nan_.*', '', regex=True, inplace=True)



#####################################################################
# READY
woo_df_ready = woo_df_1.copy()



#####################################################################
# process woo data for finance

df_woo_final = pd.DataFrame(index=woo_df_ready.index)
df_woo_final['Order ID'] = 'W' + woo_df_ready.order_id.astype('Int64').astype(str)
df_woo_final['Order Number'] = woo_df_ready.order_id.astype('Int64').astype(str)
df_woo_final['Source'] = 'Woo'
df_woo_final['Patient ID'] = woo_df_ready['_billing_phone_cleaned'].values
df_woo_final['Booked For'] = (woo_df_ready['_shipping_first_name'] + ' ' + woo_df_ready['_shipping_last_name']).str.strip()
df_woo_final['Agent ID'] = ''
# df_woo_final['Agent Name'] = woo_df_ready['user_display_name'].fillna('')
df_woo_final['Agent Name'] = woo_df_ready['comment_author'].fillna('')
df_woo_final['Booked By'] = df_woo_final['Booked For']
df_woo_final['Phone Number'] = woo_df_ready['_billing_phone_cleaned']
# df_woo_final['Birthday'] = ''
# df_woo_final['Gender'] = ''
# df_woo_final['Clinic ID'] = ''
# df_woo_final['Clinic Name'] = ''
# df_woo_final['Doctor ID'] = ''
# df_woo_final['Doctor Name'] = ''
df_woo_final['Schedule Date'] = woo_df_ready['created_at']
df_woo_final['Schedule Time'] = pd.to_datetime(woo_df_ready['created_at']).dt.strftime('%H:%M:%S')
df_woo_final['Note'] = woo_df_ready['customer_note']
df_woo_final['Nurse Name'] = woo_df_ready['nurse_name']
df_woo_final['Create Date'] = pd.to_datetime(woo_df_ready['created_at'])
# df_woo_final['Mode'] = ''
# df_woo_final['Apt Status'] = ''
df_woo_final['Order Status'] = woo_df_ready['status']
df_woo_final['Delivery Status'] = woo_df_ready['Delivery Status']
# df_woo_final['Lab Status'] = ''
df_woo_final['Payment Method'] = woo_df_ready['_payment_method']
df_woo_final['Payment ID'] = woo_df_ready.index.astype('Int64').astype(str)
df_woo_final['Payment Date'] = pd.to_datetime(woo_df_ready['_paid_date'])
# df_woo_final['Payment Status'] = woo_df_ready['original_fee_status']
df_woo_final['Item ID'] = woo_df_ready['product_id']
df_woo_final['Item Name VI'] = woo_df_ready['name_vi']
df_woo_final['Item Name EN'] = np.where(woo_df_ready.name_en.isna(), woo_df_ready.order_item_name, woo_df_ready.name_en)

# Item Name Group (manual)
### category data is imcomplete because (1) 1 product might have multiple categories; (2) old/modified products do not have categories => have to group manually
# item_name = df_woo_final[['Item Name EN']].drop_duplicates()
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=0', item_name, 'Sheet2')
gr = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1YAfQ92taXszz_AW9IGJP9plPtAWcbvtIFHSyqQ7CDqA/edit#gid=372114713')
gr = gr.dropna(axis=0, subset=['Item Name Group', 'Item Name EN']).dropna(axis=1)
df_woo_final = df_woo_final.join(gr.set_index('Item Name EN'), on='Item Name EN')

df_woo_final['Item SKU'] = woo_df_ready['item_sku']
df_woo_final['Item Quantity'] = woo_df_ready['_qty'].astype('Int64')
df_woo_final['Item Subtotal'] = woo_df_ready['_line_subtotal'].astype(float).astype('Int64')
df_woo_final['Item Unit Price'] = (df_woo_final['Item Subtotal'] / df_woo_final['Item Quantity']).astype('Int64')
df_woo_final['Shipping Total'] = woo_df_ready['_order_shipping'].astype('Int64') + woo_df_ready['_order_shipping_tax'].astype('Int64')
df_woo_final['Nurse Fee'] = woo_df_ready['nurse_fee'].astype('Int64')
df_woo_final['Tax Total'] = woo_df_ready['total_tax'].astype('Int64')
# df_woo_final['Tax Rate'] = 0
df_woo_final['Discount Rate'] = woo_df_ready['coupon_rate']
df_woo_final['Discount Fixed Amount'] = woo_df_ready['coupon_amount']
df_woo_final['Discount Total'] = (
    df_woo_final['Discount Rate'].fillna(0) * df_woo_final['Item Subtotal']
    + df_woo_final['Discount Fixed Amount'].fillna(0)
    )
df_woo_final['Refund Amount'] = woo_df_ready['_refund_amount'].astype('Int64')
df_woo_final['Refund Reason'] = woo_df_ready['_refund_reason']
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
# df_woo_final['B2B'] = woo_df_ready['B2B']
df_woo_final['Item Category ID'] = woo_df_ready['category_id']
df_woo_final['Item Category VI'] = woo_df_ready['category_name']
df_woo_final['Item Category EN'] = woo_df_ready['category_slug']
df_woo_final['Type Report'] = woo_df_ready['Type Report']
df_woo_final['Currency'] = woo_df_ready['_order_currency']
df_woo_final['Shipping Method'] = woo_df_ready['shipping_method_name']
df_woo_final['Company Name'] = woo_df_ready['_billing_company']
df_woo_final['Email'] = woo_df_ready['_billing_email']
df_woo_final['Address'] = (
    woo_df_ready['_shipping_address_1']
    + ' | ' + woo_df_ready['_shipping_city']
    + ' | ' + woo_df_ready['_shipping_postcode']
    + ' | ' + woo_df_ready['_shipping_country']
    )



##############################################################################
##############################################################################
# LOAD
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/1AhariGN_ISezVTDMpD-hmzJbPyA4nEp8s782mLBiWWc/edit#gid=0', df_woo_final)

# df_woo_final[df_woo_final['Order ID'].str.contains('W16353')]
# woo_df_ready.columns.str.contains('agent')
# woo_df_ready[woo_df_ready['order_id']==16353]

