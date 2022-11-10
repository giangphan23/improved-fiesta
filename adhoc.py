import numpy as np
import pandas as pd
import docosan_module as domo

df = domo.sql_to_df('active_accounts.sql')
domo.update_gsheet('https://docs.google.com/spreadsheets/d/185PireHvhCKT9DE_H47m5VfhlJXQJjP9R99pBMCoVjo/', df, 'active_accounts_raw')
