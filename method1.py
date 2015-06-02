import pandas as pd
import numpy as ny
import os
from dateutil import parser
import datetime
import csv
from sklearn import linear_model
import statsmodels.api
import statsmodels.formula.api as smf
from pandas.stats.api import ols

path =r'/Volumes/MyPassport/242data' 
os.chdir(path) #change working directory
# get the paths for fare data and trip data
files_fare = !ls *fare*.csv
files_trip = !ls *data*.csv
## loop over all the data to get columns we want
trip_fare_all= pd.concat([pd.read_csv(f, sep = ",",error_bad_lines = False,usecols = [6,9,10]) for f in files_fare])
trip_fare_all.columns = ['surcharge','tolls_amount','total_amount']
all_trip= pd.concat([pd.read_csv(g, sep = ",",error_bad_lines = False,usecols = [5,6],parse_dates = [5,6],skiprows = [0],header=None) for g in files_trip])
all_trip.columns = ["pickup_datetime","dropoff_datetime"]

### calculate time_sec using dropoff_datetime - pickup_data_time

time_sec = (all_trip['dropoff_datetime'] - all_trip['pickup_datetime'])/ny.timedelta64(1,'s')

#### calculate total_amount - tolls_amount
total_minus_tolls = trip_fare_all["total_amount"] - trip_fare_all["tolls_amount"]

## calculate quantiles for total - tolls;
total_minus_tolls.sort(ascending=True)
deci = ny.linspace(0.1,1,10)
total_minus_tolls.quantile(deci)

### combine all columns for the regression
surcharge = trip_fare_all["surcharge"]
result = pd.DataFrame(total_minus_tolls)
result['surcharge'] = surcharge.tolist()
result['time_sec'] = time_sec.tolist()
result.columns = ['amount','surcharge','time_sec']

### filter some implausible observations
filter_result = result[(result['time_sec'] > 0) & (result['amount'] > 0) & (result['amount'] < 100)]

    
#### simple regression
model = smf.ols('amount ~ time_sec', data = filter_result)
fit = model.fit()
fit.summary2()

### multiple regression
model_mul = smf.ols('amount ~ time_sec + surcharge', data = filter_result)
fit_mul = model_mul.fit()
fit_mul.summary2()









