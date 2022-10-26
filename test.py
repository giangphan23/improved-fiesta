import docosan_module as domo

df = domo.load_gsheet('https://docs.google.com/spreadsheets/d/1ofUHnklPsz3TjImPqIpc7ctPMDHsxRmNGv9DQRRrduo/edit#gid=0')
df.head()

domo.update_gsheet('https://docs.google.com/spreadsheets/d/1_o-6K3yaRUGeyUSeySaPG2HS2zwus7HZN1cZqQ6BVKE/edit#gid=0', df.head())

# done
# clone finished!
