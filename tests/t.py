import yfinance as yf
import datetime as dt




start_date = dt.date.today() - dt.timedelta(days=1)
end_date = start_date + dt.timedelta(days=1)
tsla_df = yf.download("TSLA", start=start_date, end=end_date,interval='1m') 
tsla_df.to_csv("data.csv", header=False)



print(tsla_df)
print(start_date)
print(end_date)