from datetime import datetime
today = datetime.today()
current_month = today.month
current_day = today.day

import pandas as pd
EXCEL_PATH = r'C:\Users\shivanandk\Downloads\data.xlsx'
df = pd.read_excel(EXCEL_PATH).dropna()
match = df[df['Month'] == current_month]
dt =  datetime.now()
today = str(dt.strftime("%d"))
# print(df['Month'],df['DaysToRun'])
# print(df['Month'] == current_month and  (df['DaysToRun'] == current_day) )

qry = f'DaysToRun == {current_day} and Month == {current_month}'
names = df.query(qry)['names'].iloc[0]
print(names)
# if df['Month'] == current_month and  (df['DaysToRun'] == current_day) :
#     print(current_month,today)
#     days_str = str(match.iloc[0]['DaysToRun'])
#     # scheduled_days = [int(day.strip()) for day in days_str.split(',') if day.strip().isdigit()]
    
#     qry = f'DaysToRun == {current_day}'
#     names = df.query(qry)['names'].iloc[0]
#     print(names)