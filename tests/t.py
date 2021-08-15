import yfinance as yf
import datetime as dt




start_date = dt.date.today() - dt.timedelta(days=20)
end_date = start_date + dt.timedelta(days=20)
tsla_df = yf.download("TSLA", start="2021-07-26", end="2021-07-27",interval='1m') 
tsla_df.to_csv("data.csv", header=False)



print(tsla_df)
print(start_date)
print(end_date)