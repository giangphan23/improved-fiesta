import numpy as np
import pandas as pd
import docosan_module as domo

# df = domo.sql_to_df('active_accounts.sql')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/185PireHvhCKT9DE_H47m5VfhlJXQJjP9R99pBMCoVjo/', df, 'active_accounts_raw')


# telemedicine doctors & private clinics
sql = '''
SELECT d.id as doc_id
    , d.display_name as doctor_name
    , d.status as doctor_status
    , d.sv_available as doctor_sv_available
    , u.phone_number as doctor_phone
    , role as doctor_role
    , c.id as clinic_id
    , c.name as clinic_name
    , c.status as clinic_status
    , c.phone as clinic_phone
    , ds.*

FROM doctor_clinic_relationship dcr
	JOIN doctors d ON dcr.doctor_id=d.id
    JOIN clinics c ON dcr.clinic_id=c.id
    LEFT JOIN users u ON d.user_id = u.id
    LEFT JOIN doctor_schedule ds on d.id = ds.doctor_id

WHERE d.display_name NOT LIKE "%test%"
    AND d.display_name NOT LIKE "%demo%"
    AND d.display_name NOT LIKE "%tesst%"
    AND d.display_name NOT LIKE "%abc%"
    AND d.display_name NOT LIKE "%k21%"
    AND d.display_name NOT LIKE "khanh%"
    AND d.display_name NOT LIKE "khanh%"
    AND c.name NOT LIKE "%abc%"
;
'''
df = domo.single_SQL_query_to_df(sql)
df.info()


#
df.set_index('doc_id', append=True, inplace=True)

lr=[]
for c in ['mon', 'tue', 'wed', 'thurs', 'fri', 'sat', 'sun']:
    lc=[]
    idx=[]
    for r in df.index:
        cell = df.loc[r,c]
        if cell != None:
            slot_count = pd.json_normalize(eval(cell)).T.sum()
            lc.append(slot_count[0])
        else:
            lc.append(None)
        idx.append(r)
    lr.append(lc)

df_sche = pd.DataFrame(lr).T
df_sche.index = pd.MultiIndex.from_tuples(idx, names=[None, 'doc_id'])

df[['mon', 'tue', 'wed', 'thurs', 'fri', 'sat', 'sun']] = df_sche
df['SUM_SLOTS'] = df[['mon', 'tue', 'wed', 'thurs', 'fri', 'sat', 'sun']].sum(axis=1)

domo.update_gsheet('https://docs.google.com/spreadsheets/d/16Sv1AqUnZ7N3U4kNVDGdKrwsl1Hp2AsajkN9jCyWcIk/', df, 'schedule', string_escaping='full')
