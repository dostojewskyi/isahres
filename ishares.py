#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 21 22:45:46 2021

@author: nsa
"""
import time
#import lxml
import requests
import numpy as np
import pandas as pd
from lxml import html
from tqdm.auto import tqdm
#from datetime import datetime

print('''Welcome to the Ishares Scraper.
      
Please follow the instructions and you are ready to go to get a millionaire.''')

inp = input('1. Enter your source path file of the Ishares Excel. Make sure the file format is "xlsx" Excel format: ' )
if inp[-1] == 'x':
    source = inp[:-5]
    postfix = inp[-5:]
if inp[-1] == 'v':
    source = inp[:-4]
    postfix = inp[-4:]
    
class IsharesScraper:
    
    # ishares_xlsx = path ishares excel, cleaned_csv = path to save cleaned isahres data, fundamentals_xlsx = path to save fundamentals
    def __init__(self, ishares_xlsx,cleaned_csv=f'{source}_cleaned.csv', fundamentals_xlsx=f'{source}_fundamentals.xlsx',postfix = postfix):
        self.postfix = postfix
        if self.postfix == '.xlsx':
            self.ishares_xlsx = pd.read_excel(f'{ishares_xlsx}.xlsx')
        elif self.postfix == '.csv':
            self.ishares_xlsx = pd.read_csv(f'{ishares_xlsx}.csv', skiprows=2)
        else:
            self.ishares_xlsx = None
        self.cleaned_csv = cleaned_csv
        self.fundamentals_xlsx = fundamentals_xlsx
        
    def file_controll(self):
        if self.ishares_xlsx == None:
            return 'Process stopped: please only use Excel (.xlsx) or CSV (.csv) files as your source'
    
    def isin_to_ticker(self):
        
        ishares = self.ishares_xlsx
        ticker = []
        url = "https://query2.finance.yahoo.com/v1/finance/search"
        
        if 'Emittententicker' in ishares.columns:
            ishares['ticker'] = ishares['Emittententicker']
            return ishares
        
        print('receive ticker symbol based on ISIN from provieded iShares Excel. Please wait.')
        for i,_ in zip(ishares['ISIN'], tqdm(ishares['ISIN'])):

            #if ISIN is -, position is no stock equity (cash, futures etc.), no fundamentals given
            if i == '-':
                ticker.append(np.nan)
                continue

            params = {'q': f'{i}', 'quotesCount': 1, 'newsCount': 0}
            r = requests.get(url, params=params)
            data = r.json()   

            #if data['quotes'] is empty, stock is not known by yahoo finance / or can't find matching ticker. Skip value
            if not data['quotes']:
                ticker.append(np.nan)
            else:
                ticker.append(data['quotes'][0]['symbol'])
            time.sleep(1)

        ishares['ticker'] = ticker
        ishares.dropna(subset=['ticker'], inplace=True)
        ishares.to_csv(self.cleaned_csv)
        return ishares
        
    def fundamentals(self,symbol,sleep):
        url = 'https://finance.yahoo.com/quote/' + symbol + '/balance-sheet?p=' + symbol

        # Set up the request headers that we're going to use, to simulate
        # a request by the Chrome browser. Simulating a request from a browser
        # is generally good practice when building a scraper
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Pragma': 'no-cache',
            'Referrer': 'https://google.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'
        }

        # Fetch the page that we're going to parse, using the request headers
        # defined above
        page = requests.get(url, headers)

        # Parse the page with LXML, so that we can start doing some XPATH queries
        # to extract the data that we want
        tree = html.fromstring(page.content)

        # Smoke test that we fetched the page by fetching and displaying the H1 element
        tree.xpath("//h1/text()")

        table_rows = tree.xpath("//div[contains(@class, 'D(tbr)')]")

        # Ensure that some table rows are found; if none are found, then it's possible
        # that Yahoo Finance has changed their page layout, or have detected
        # that you're scraping the page.
        
        #edit: if scaping detected, sleep increases by sleep**2
        if len(table_rows) == 0:
            time.sleep(sleep)
            return pd.DataFrame()
            
        
        parsed_rows = []

        for table_row in table_rows:
            parsed_row = []
            el = table_row.xpath("./div")
    
            none_count = 0
    
            for rs in el:
                try:
                    (text,) = rs.xpath('.//span/text()[1]')
                    parsed_row.append(text)
                except ValueError:
                    parsed_row.append(np.NaN)
                    none_count += 1

            if (none_count < 4):
                parsed_rows.append(parsed_row)

        df = pd.DataFrame(parsed_rows)

        df = pd.DataFrame(parsed_rows)
        df = df.set_index(0) # Set the index to the first column: 'Period Ending'.
        df = df.transpose() # Transpose the DataFrame, so that our header contains the account names

        # Rename the "Breakdown" column to "Date"
        cols = list(df.columns)
        cols[0] = 'Date'
        df = df.set_axis(cols, axis='columns', inplace=False)

        numeric_columns = list(df.columns)[1::] # Take all columns, except the first (which is the 'Date' column)

        for column_name in numeric_columns:
            df[column_name] = df[column_name].str.replace(',', '') # Remove the thousands separator
            df[column_name] = df[column_name].astype(np.float64) # Convert the column to float64

        df['ticker'] = symbol
        time.sleep(sleep)
        return df
    
    def run_scrapper(self):
        #get table of tickers
        sleep = 1
        ishares = self.isin_to_ticker()
        symbol = ishares.ticker[1]
        df = self.fundamentals(symbol,sleep)
        #define empty DF with fitting Columns 
        yahoo = pd.DataFrame(columns=df.columns)
        
        print('scraping yahoo finance. Will take some time, please grab some coffee')
        for i,_ in zip(ishares.ticker, tqdm(ishares.ticker)):
            
            df = self.fundamentals(i,sleep)
            while df.empty:
                sleep += sleep
                print(f'yahoo finance detected scraping. Increased delay by {sleep}. Continue after a break of {60} seconds with and updated delay of {sleep} seconds. ')
                time.sleep(60)
                df = self.fundamentals(i,sleep)
                
            yahoo = yahoo.append(df)
            
        fundamentals = pd.merge(ishares, yahoo, how="left", on=["ticker"])
        fundamentals.set_index(['Name', 'Date'], inplace=True)
        fundamentals.to_excel(self.fundamentals_xlsx)

        print("process finished - you'r almost a millionaire. Your file is located in the folder of the source data.")
        
        #print('scraping yahoo fince, grab some coffee')
        #for i,_ in zip(ishares.ticker, tqdm(ishares.ticker)):

if input('start scrapint? y/n ' ) == 'y':
        IsharesScraper(f'{source}', f'{source}_cleaned.csv', f'{source}_fundamentals.xlsx').run_scrapper()
else:
    print('process ended')
    exit(5)


