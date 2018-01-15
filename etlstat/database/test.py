import pandas as pd

labels2 = {'Descripción': 'description' , 'Pie de página': 'documentation'}
title = 'Descripción'
desc = 'Caza'
uri = 'uri-tag'

update_columns = []
update_columns.append(labels2[title])
update_columns.append('uri_tag')

table = pd.DataFrame(columns=update_columns)
table = table.append({labels2[title]: desc, 'uri_tag': uri}, ignore_index=True)
table.name = 'time_series'

print(table)