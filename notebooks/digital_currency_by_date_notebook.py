#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'notebooks'))
	print(os.getcwd())
except:
	pass
#%% [markdown]
# # Data loading notebook
#%% [markdown]
# ## Import packages

#%%
import pandas as pd
import requests
import io
import time
import matplotlib.pyplot as plt

#%% [markdown]
# ## Initialization

#%%
API_ENDPOINT = "https://www.alphavantage.co/query"
API_FUNCTION = "DIGITAL_CURRENCY_DAILY"
API_DATATYPE = "csv"

API_KEY = "OWGOAH1MLEK1J3IA" # Reset every 500 requests from [here](https://www.alphavantage.co/support/#api-key)
plt.close('all')

#%% [markdown]
# ## Extracting and transforming data
#%% [markdown]
# #### Retrieve data from API

#%%
params = {'function': API_FUNCTION, 'market': 'USD', 'symbol': 'BTC', 'apikey': API_KEY, 'datatype': API_DATATYPE}
start = time.time()
list_ = []

url_data = requests.get(API_ENDPOINT, params=params).content

df = pd.read_csv(io.StringIO(url_data.decode('utf-8')),
        header=0,
        index_col=['timestamp'], 
        usecols=['timestamp', 'open (USD)', 'close (USD)'],
        parse_dates=['timestamp'],
        nrows=30)
                     
# time.sleep(12) # 5 API requests per minute at most
end = time.time()
elapsed = end - start
print("---------- Elapsed time: {0}".format(elapsed))

df

#%% [markdown]
# #### Export plot

#%%
fig = df.plot(title="Open and close prices of BTC in last 30 days", grid=True).get_figure()

fig.savefig('figure.pdf')

#%% [markdown]
# #### Average difference

#%%
abs(df['open (USD)'] - df['close (USD)'])


#%%
sum(abs(df['open (USD)'] - df['close (USD)']))/30


