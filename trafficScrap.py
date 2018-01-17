import json
import requests
from datetime import datetime, timedelta
from timer import *
import pandas as pd
import os
from trafficDb import *

API_KEY = "AIzaSyB04IaxuKUYF85SizuDSs5C4GqbcUuJU7I"
MODEL = "best_guess"
DIR= os.path.dirname(os.path.abspath(__file__))

MASTER_URL="https://maps.googleapis.com/maps/api/distancematrix/json"

def getUrlResult(origin, destination, apikey= API_KEY, url=MASTER_URL, model= MODEL):
    params = dict(
        origins="%s,%s" % origin,
        destinations="%s,%s" % destination,
        key= apikey,
        traffic_model= model,
        departure_time= 'now',
        mode='driving'
    )
    return requests.get(url, params)


def loadRoute(filePath):
    try:
        open(filePath)
    except:
        print(filePath, 'cannot be opened')
        return
    df= pd.read_csv(filePath)
    df.name= filePath.split('/')[-1].split('.')[-2]
    return df


def _exportRawData(data, filePath):
    with open(filePath, 'w') as outFile:
        json.dump(data, outFile)
    return


def exportDatas(datas, fileDir='', name=''):
    if datas is None:
        print('result list is empty')
        return

    for i, data in enumerate(datas):
        filePath= '%s/%s-%s-%s.json' % ( fileDir, name, str(i), datetime.now().strftime('%Y%m%d-%H%M%S') )
        _exportRawData(data, filePath)
    return


def getRouteDatas(route):
    datas= list()
    if not isinstance(route, pd.DataFrame):
        print('route input must be DataFrame')
        return

    wp_count= route.shape[0]
    for i in range(0, wp_count-1):
        ori= (route['lat'][i], route['lon'][i])
        dest= (route['lat'][i+1], route['lon'][i+1])

        data= getUrlResult(ori, dest).json()
        data['time']= datetime.now().strftime('%Y%m%d%H%M%S')
        data['origin_coord']=  ori
        data['destination_coord']= dest

        datas.append(data)
    return datas



if __name__== '__main__':
    db= init_db(os.path.join(DIR, 'traffic_db.sqlite'), clean=False)
    startTime= findNextIntervalMinute(15) #set time to start at next quarter
    interval= timedelta(minutes=15)
    print('Data collection begins at:', startTime)
    print('Interval = ', interval)
    print('-'*50)
    sleepUntil(startTime)

    i=0
    while True:
        i+=1
        print('Collecting dataset %s at %s' % (i, datetime.now()) )
        route= loadRoute(r'_ROUTE/taipo-taiwai.csv')
        datas= getRouteDatas(route)
        exportDatas(datas, r'_RAW', name='taipo-taiwai')
        exportDatas_db(db, datas)

        route= loadRoute(r'_ROUTE/taiwai-taipo.csv')
        datas= getRouteDatas(route)
        exportDatas(datas, r'_RAW', name='taiwai-taipo')
        exportDatas_db(db, datas)

        wakeAtInterval(interval)
