import yfinance as yf
import pandas as pd
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta

def fetch_stock_data(ticker):
    
    # Fetching the past 10 days data to ensure a full week of trading days
    end_date = datetime.today()
    start_date = end_date - timedelta(days=10)
    
    stock_data = yf.download(ticker, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), progress=False)
    stock = yf.Ticker(ticker)
    
    # Fetch additional company info
    info = stock.info
    company_data = {
        "symbol": ticker,
        "longName": info.get("longName", "N/A"),
        "longBusinessSummary": info.get("longBusinessSummary", "N/A"),
        "sector": info.get("sector", "N/A"),
        "industry": info.get("industry", "N/A"),
        "totalRevenue": info.get("totalRevenue", 0),
        "marketCap": info.get("marketCap", 0),
        "bookValue": info.get("bookValue", 0),
        "priceToBook": info.get("priceToBook", 0),
        "dividendYield": info.get("dividendYield", 0),
        "returnOnEquity": info.get("returnOnEquity", 0),
        "priceToSalesTrailing12Months": info.get("priceToSalesTrailing12Months", 0),
        "recommendationKey": info.get("recommendationKey", "N/A")
    }
    
    return stock_data, company_data

def send_to_kafka(stock_data, company_data, kafka_topic):
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        
        for index, row in stock_data.iterrows():
            message = {
                'date': index.strftime('%Y-%m-%d'),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'company_info': company_data
            }
            producer.send(kafka_topic, message)
        
        producer.flush()
    except Exception as e:
        print(f"An error occurred while sending data to Kafka: {e}")

if __name__ == "__main__":
    companies = ["GOOGL"] #In this case, we are only fetching data for Google(MVP)
    
    for company in companies:
        try:
            stock_data, company_data = fetch_stock_data(company)
            kafka_topic = "stock_data"
            send_to_kafka(stock_data, company_data, kafka_topic)
        except Exception as e:
            print(f"An error occurred for company {company}: {e}")
