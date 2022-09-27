import pandas as pd
url = "https://raw.githubusercontent.com/data-eng-10-21/window-functions/main/favorita_transactions.csv"
df = pd.read_csv(url)
df[:2]

import sqlite3
conn = sqlite3.connect('grocery.db')

df.to_sql('store_transactions', conn, index = False, if_exists = 'replace')

print('display sales data')
query = 'SELECT * FROM store_transactions LIMIT 5'
print(query)
print(pd.read_sql(query, conn))
print()
print()

print('use a window function to calculate the total amount sold by store')
query = """SELECT date, store_nbr, transactions,
SUM(transactions) OVER (partition by store_nbr) as total
FROM store_transactions
 LIMIT 6"""
print(query)
print(pd.read_sql(query, conn))
print()
print()

print('use a window function to calculate the running sold by each store')
query = """SELECT date, store_nbr, transactions,
SUM(transactions) OVER (partition by store_nbr ORDER BY date) as running_total
FROM store_transactions
 LIMIT 6"""
running_total_df = pd.read_sql(query, conn)
print(query)
print(running_total_df)
print()
print()


import plotly.graph_objects as go

running_total_scatter = go.Scatter(x = running_total_df.date,
                                   y = running_total_df.running_total)
print(go.Figure(running_total_scatter, layout = dict(title = 'Total sales through first week')))

print("""'how much are we selling collectively, and how much is each store contributing to the daily quota""")
query = """SELECT date, store_nbr, transactions,
SUM(transactions) OVER (PARTITION BY date ORDER BY transactions DESC) as running_total
FROM store_transactions
LIMIT 100
"""
print(query)
daily_quota_df = pd.read_sql(query, conn)
print(daily_quota_df)
