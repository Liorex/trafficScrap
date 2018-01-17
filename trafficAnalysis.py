import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

DIR= os.path.dirname(os.path.abspath(__file__))

def extract_trafficData(db_path):
    db= sqlite3.connect(db_path)
    cur= db.cursor()

    query='SELECT * FROM TrafficData'
    df= pd.read_sql(query, db, index_col='id', parse_dates=['recordTime'])
    db.close()
    return df


def extract_waypointData(db_path):
    db= sqlite3.connect(db_path)
    cur= db.cursor()

    query='SELECT * FROM Waypoint'
    df= pd.read_sql(query, db)
    db.close()
    return df


def extract_routeFolder(fileDir):
    ## extract route info from folder
    route= pd.DataFrame()
    print('-'*50)
    print('Extracting route files:')

    for i,f in enumerate(os.listdir(fileDir)):

        if f.endswith('.csv'):
            filePath=fileDir+ '/'+ f
            print(filePath)
            rt= pd.read_csv(filePath)
            rt['route']=i
            route= pd.concat([route, rt], axis=0)

    print('-'*50)
    print()
    return route


def compile_data(traffic, route, waypoint):
    ## merge route and waypoint data into traffic data
    ## return final dataFrame containing all stored data
    trff= traffic
    rt= route
    wp= waypoint
    rt= pd.merge(rt, wp, on=['lat','lon'], how='outer')
    trff= pd.merge(trff, rt.add_prefix('origin_'), on=['origin_id'], how='outer')
    trff= pd.merge(trff, rt.add_prefix('destination_'), on=['destination_id'], how='outer')

    # clean table
    trff= trff.drop('destination_route', axis=1)
    trff= trff.rename(columns={'origin_route':'route'})
    return trff


def clean_data(data_input):
    data= data_input
    ## prepare a dataFrame for analysis and plotting
    # form route segment name
    df= pd.DataFrame()
    data= data.dropna()
    df['segment']= data.origin_id.map(int).map(str).str.zfill(2)+ ') '+ data.origin_name_eng.map(str) +' - '+ data.destination_name_eng.map(str)
    df['recordTime']= data.recordTime.apply(lambda x:x.replace(second=0))
    df['driveTime']= data.driveTime /60 # convert to minutes
    df['route']= data.route.map(int)

    return df


#===========================================
if __name__=='__main__':
    trff= extract_trafficData('traffic_db.sqlite')
    wp= extract_waypointData('traffic_db.sqlite')
    rt= extract_routeFolder(DIR+'/_ROUTE')
    data= compile_data(traffic=trff, route=rt, waypoint=wp)

    df= clean_data(data)
    print(df.head())
