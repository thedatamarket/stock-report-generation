import os
import pandas as pd
import numpy as np
import wget
import zipfile
from datetime import datetime, date,timedelta


# URL = 'https://www1.nseindia.com/content/equities/EQUITY_L.csv'
# response = wget.download(URL, "ticker.csv")
count = 0
print('downloading......')
try:
    # dt = date.today().strftime("%d%b%Y").upper() 
    str1 = date.today() - timedelta(days = 5)
    dt = str1.strftime("%d%b%Y").upper() 
    mon = date.today().strftime("%b").upper()
    str1 = 'cm' + str(dt) +'bhav.csv.zip'
    year = date.today().strftime("%Y").upper()
    print(str1)
    URL = 'https://www1.nseindia.com/content/historical/EQUITIES/'+str(year)+'/' + str(mon) + '/' + str1
    response = wget.download(URL, 'Test/'+str1)
    with zipfile.ZipFile('Test/'+str1, 'r') as zip_ref:
        zip_ref.extractall('extracted/')
except:
    pass

print('processing.....')
path = "extracted"
dir_list = sorted(os.listdir(path))
dir_list1 = sorted(os.listdir("output"))
# dir, dir1, dir2 = [], [], []
# for i in range(len(dir_list)):
#   dir.append(dir_list[i].split('cm')[1].split('bhav')[0])
#   datetime_object = datetime.strptime(dir[i], '%d%b%Y')
#   dir1.append(datetime_object.date())

# dir1 = sorted(dir1)
# for i in range(len(dir1)):
#   d = dir1[i].strftime("%d%b%Y")
#   fin = 'cm'+d.upper()+'bhav.csv'
#   dir2.append(fin)
# dir_list = dir2
# dir_list.reverse()
ticker = pd.read_csv(dir_list1[0])
ticker = ticker[["SYMBOL"]]
df2 = ticker.merge(ticker, on='SYMBOL', how='left')
for i in dir_list:
  path = "extracted/"+str(i)
  prevClose = 'PREVCLOSE_'+str(i)
  close = 'CLOSE_'+str(i)
  df = pd.read_csv(path)
  df = df[df['SERIES'] == "EQ"]
  df.rename(columns = {'PREVCLOSE':prevClose, 'CLOSE':close}, inplace = True)
  df[i] = ((df[close] - df[prevClose])/df[prevClose]) * 100
  df = df[["SYMBOL", i]]
  df2 = df2.merge(df, on='SYMBOL', how='left')

#save file
today = date.today()
d4 = today.strftime('%b-%d-%Y')
filename = str(d4) + '.csv'
df2.to_csv(filename)
print('Process completed... file generated ', filename)

#add market cap at column
#add current price column
#52weeks high and low col
#type of industry