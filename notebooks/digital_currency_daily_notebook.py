#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'notebooks'))
	print(os.getcwd())
except:
	pass
#%% [markdown]
# # Data loading notebook
# 
#%% [markdown]
# ## Import packages

#%%
import pandas as pd
import requests
import io
import time

#%% [markdown]
# ## Constants

#%%
SYMBOL_LIST_URL = "https://www.alphavantage.co/digital_currency_list/"
MARKET_LIST_URL = "https://www.alphavantage.co/physical_currency_list/"
API_ENDPOINT = "https://www.alphavantage.co/query"
API_FUNCTION = "DIGITAL_CURRENCY_DAILY"
API_DATATYPE = "csv"

API_KEY = "OWGOAH1MLEK1J3IA" # Reset every 500 requests from [here](https://www.alphavantage.co/support/#api-key)

#%% [markdown]
# ## Reading data

#%%
symbol=pd.read_csv(SYMBOL_LIST_URL)
market=pd.read_csv(MARKET_LIST_URL)

#%% [markdown]
# ### Understanding data

#%%
symbol.columns


#%%
market.dtypes


#%%
# ((rows, columns), (rows, columns))
symbol.shape, market.shape

#%% [markdown]
# ### Extracting and transforming data
# 
# "Prices and volumes are quoted in both the market-specific currency **and USD**", restricting market to USD only.
# 
# "5 API requests per minute; **500 API requests per day**", restricting requests to first 500 symbols
#%% [markdown]
# #### Retrieve data from API

#%%
params = {'function': API_FUNCTION, 'market': 'USD', 'apikey': API_KEY, 'datatype': API_DATATYPE} # Only USD
limit = 500
symbol = symbol.head(limit)
start = time.time()
print("Starting to read first {0} symbols...".format(limit))
list_ = []

for s_index, s_row in symbol.iterrows():
    params['symbol'] = s_row['currency code']
    url_data = requests.get(API_ENDPOINT, params=params).content
    df = pd.read_csv(io.StringIO(url_data.decode('utf-8')), index_col=None, header=0, nrows=1)
    if len(df.columns) > 1:
        df.insert(0, 'currency code', s_row['currency code'], allow_duplicates=False)
        list_.append(df)

    time.sleep(12) # 5 API requests per minute at most
    end = time.time()
    elapsed = end - start
    remaining = limit * 13 - elapsed
    print("---------- Index: {0}, Elapsed time: {1}, Estimated remaining time: {2}".format(s_index, elapsed, remaining))

frame = pd.concat(list_, axis = 0, ignore_index = True)
frame

#%% [markdown]
# #### Remove duplicated columns

#%%
frame.drop(['open (USD).1', 'high (USD).1', 'low (USD).1', 'close (USD).1'], axis=1, inplace=True)
frame

#%% [markdown]
# #### Export to csv

#%%
timestamp = frame.iloc[0]['timestamp']
frame.to_csv(path_or_buf="../output_data/{0}.csv".format(timestamp), index=False)


