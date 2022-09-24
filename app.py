from flask import Flask
import os
import pandas as pd
import numpy as np
import wget
import zipfile
from datetime import datetime, date
app = Flask(__name__)

@app.route('/')
def hello_world():
    URL = 'https://www1.nseindia.com/content/equities/EQUITY_L.csv'
    response = wget.download(URL, "ticker.csv")
    count = 0
    year = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
    year = ['SEP']
    month = ['2022']
    print('downloading......')
    for p in month:
        for j in year:
            for i in range(1,31):
                try:
                    year = p
                    dt = str(i) + j + year
                    # print(dt)
                    if dt == '6AUG2022':
                        exit()
                    if i < 10:
                        str1 = 'cm0' + dt +'bhav.csv.zip'
                    else:
                        str1 = 'cm' + dt +'bhav.csv.zip'
                    print(str1)
                    URL = 'https://www1.nseindia.com/content/historical/EQUITIES/'+year+'/' + j + '/' + str1
                    response = wget.download(URL, 'Test/'+str1)
                    with zipfile.ZipFile('Test/'+str1, 'r') as zip_ref:
                        zip_ref.extractall('extracted/')
                except:
                    pass

    print('processing.....')
    path = "extracted"
    dir_list = sorted(os.listdir(path))
    dir, dir1, dir2 = [], [], []
    for i in range(len(dir_list)):
        dir.append(dir_list[i].split('cm')[1].split('bhav')[0])
        datetime_object = datetime.strptime(dir[i], '%d%b%Y')
        dir1.append(datetime_object.date())

    dir1 = sorted(dir1)
    for i in range(len(dir1)):
        d = dir1[i].strftime("%d%b%Y")
        fin = 'cm'+d.upper()+'bhav.csv'
        dir2.append(fin)
        dir_list = dir2
        dir_list.reverse()
        ticker = pd.read_csv("ticker.csv")
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

if __name__ == "__main__":
    app.run(debug=True, port=10000)