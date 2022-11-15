import numpy as np
import pandas as pd
import docosan_module as domo

# df = domo.sql_to_df('active_accounts.sql')
# domo.update_gsheet('https://docs.google.com/spreadsheets/d/185PireHvhCKT9DE_H47m5VfhlJXQJjP9R99pBMCoVjo/', df, 'active_accounts_raw')


sql = '''
SELECT d.id as doctor_id
    , display_name as doctor_name
    , u.phone_number
    , location
    , sv_available
    , d.type as doctor_type
FROM doctors d
    JOIN users u ON d.user_id = u.id
WHERE sv_available LIKE "%telemed%"
    AND display_name NOT LIKE "%demo%"
;
'''
df = domo.single_SQL_query_to_df(sql)
domo.update_gsheet('https://docs.google.com/spreadsheets/d/16Sv1AqUnZ7N3U4kNVDGdKrwsl1Hp2AsajkN9jCyWcIk/edit#gid=0', df, 'telemedicine doctors', string_escaping='full')

sql = '''
SELECT doctor_id
    , d.display_name as doctor_name
    , u.phone_number as doctor_phone
    , role as doctor_role
    , clinic_id
    , c.name as clinic_name
    , c.phone as clinic_phone

FROM doctor_clinic_relationship dcr
	JOIN doctors d ON dcr.doctor_id=d.id
    JOIN clinics c ON dcr.clinic_id=c.id
    LEFT JOIN users u ON d.user_id = u.id

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
df.shape

domo.update_gsheet('https://docs.google.com/spreadsheets/d/16Sv1AqUnZ7N3U4kNVDGdKrwsl1Hp2AsajkN9jCyWcIk/edit#gid=0', df, 'clinics', string_escaping='full')
