import numpy as np
import pandas as pd
import docosan_module as domo

# extract data
df_apt = domo.sql_to_df('SQL/Appointments.sql')

##########################################################
# Unconfirmed Appointments

col = ['Clinic ID', 'Clinic Name', 'Appointment ID', 'Patient Name', 'Requester', 'Reason', 'Create Date', 'Appointment Date', 'Appointment Time']
df_unc_apt = df_apt.loc[df_apt['Status'] == 'Unconfirmed', col]

# save file
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1iZl0X-ZM7gZ3F4sWXmYv8Ea-0ztk6-W3TTvKlyMzgAk/edit#gid=0', df_unc_apt)


##########################################################
# Content Marketing
df_cli_spec = domo.sql_to_df('SQL/clinic_with_primary_spec.sql')
df_con_mar = df_apt.merge(df_cli_spec, 'left', left_on='Clinic ID', right_on='clinic_id').drop(['clinic_id', 'clinic_name'], axis = 1).sort_values('Appointment ID')

# save file
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1_wBz3oXbsggEVaWJL7fzn1_71KCWo3U-odwJXBAEpNQ/edit#gid=0', df_con_mar)


##########################################################
# Tele-consultations

# filter
col = ['Appointment ID', 'Clinic Name', 'Doctor Name', 'Patient Name', 'Patient ID', 'Phone Number', 'Reason', 'Create Date', 'Appointment Date', 'Appointment Time']

c1 = df_apt['Appointment Mode'] == 'telemedicine'
c2 = df_apt['Clinic ID'] == 684
df_telecon = df_apt.loc[c1 & c2, col]

# remove '+' prefix in phone number
df_telecon['Phone Number'] = df_telecon['Phone Number'].str.replace('+','')

# save file
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1VC8a9gUTHu0Q5JAFsaCQll3GEmtrCphRu0nOdO-jU2w/edit#gid=0', df_telecon)




##########################################################
# Unique Users



##############
# appointments

# get data
df_uni_user = df_apt.copy()

# a customer is new when Patient ID is not NA & is unique
df_uni_user['is_new_customer'] = np.where(df_uni_user['Patient ID'].notna() & ~df_uni_user['Patient ID'].duplicated(), True, False)

# sort by ID
df_uni_user = df_uni_user.sort_values('Appointment ID', ascending=False).reset_index(drop=True)

# save file
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1dldpKGJ1vUu6P1dNxiMRrwtj0WybpWQnXazOp64ANEQ/edit#gid=0', df_uni_user)


##############
# woo orders

# # get data
# all_pages = domo.get_pages()
# df_woo_order = pd.DataFrame.from_records(all_pages)


# # preprocess woo orders
# df_woo_order_1 = domo.filter_test_and_internal(df_woo_order)
# df_woo_order_1 = domo.pivot_line_items(df_woo_order)
# df_woo_order_1 = domo.pivot_billing(df_woo_order_1)

# # drop columns containing invalid values
# col_to_drop = [
#     # dicts:
#     'billing', 'shipping','meta_data','line_items','_links',
#     # empty lists:
#     'tax_lines', 'shipping_lines', 'fee_lines', 'coupon_lines', 'refunds', 'line_items_taxes', 'line_items_meta_data'
#     ]
# df2 = df_woo_order_1.drop(col_to_drop, axis=1)

# # ADD F1 COLUMN (aka ROW INDEX)
# df3 = df_woo_order_1.reset_index().rename({'index':''}, axis=1) # df3 = wc_order_live

# temp use bc API has issues
df3 = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1wvHxPkZ8wXgplRJDbdndyJjKbS7E2ZEhDTiOAQo6umk/edit#gid=1697317978')

col = [
    'id', 'status', 'currency', 'prices_include_tax', 'date_created', 'date_modified', 'discount_total', 'discount_tax', 'shipping_total', 'shipping_tax', 'total','total_tax', 'billing', 'shipping', 'payment_method', 'payment_method_title',  'created_via', 'customer_note', 'date_completed', 'date_paid', 'meta_data', 'line_items', 'tax_lines', 'shipping_lines', 'fee_lines', 'coupon_lines', 'refunds', 'payment_url', '_links', 'test_and_internal', 'line_items_id', 'line_items_name', 'line_items_product_id', 'line_items_variation_id', 'line_items_quantity', 'line_items_tax_class', 'line_items_subtotal','line_items_subtotal_tax', 'line_items_total', 'line_items_total_tax', 'line_items_taxes', 'line_items_meta_data', 'line_items_sku', 'line_items_price', 'line_items_parent_name', 'billing_first_name', 'billing_last_name', 'billing_company', 'billing_address_1', 'billing_address_2', 'billing_city', 'billing_state', 'billing_postcode', 'billing_country', 'billing_email', 'billing_phone','billing_customer_id_'
]
df4 = df3[col]

# add col is_new_customer: True when billing_customer_id_ is not NA & is unique
df4.loc[:,['is_new_customer']] = np.where(df4['billing_customer_id_'].notna() & ~df4['billing_customer_id_'].duplicated(), True, False)

# update
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1lDtzBgyrNa7hCLjZ0S1nWmtHf992V038fckSyRIx6-s/edit#gid=0', df4)

