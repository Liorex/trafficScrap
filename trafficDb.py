import sqlite3
import pandas as pd
from datetime import datetime
import os
import json

DIR= os.path.dirname(os.path.abspath(__file__))

def _connect_db (db_name):
    db= sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES)
    return db


def dbExists (db_name):
    try:
        sqlite3.connect('file:%s?mode=rw', uri=True)
        exists=True
    except:
        exists=False
    return exists


def init_db (db_name, clean=False):
    if dbExists(db_name):
        db= _connect_db(db_name)
    else:
        db= _connect_db(db_name)
        _init_table(db, clean)

    return db


def _init_table(db, clean):
    cur= db.cursor()

    if clean:
        cur.executescript('''
            DROP TABLE IF EXISTS Waypoint;
            DROP TABLE IF EXISTS TrafficData
        ''')

    cur.executescript('''
        CREATE TABLE IF NOT EXISTS Waypoint(
            id INTEGER NOT NULL PRIMARY KEY UNIQUE,
            lat REAL,
            lon REAL,
            UNIQUE(lat, lon)
        );

        CREATE TABLE IF NOT EXISTS TrafficData(
            id INTEGER NOT NULL PRIMARY KEY UNIQUE,
            recordTime TIMESTAMP,
            origin_id INTEGER,
            destination_id INTEGER,
            driveTime INTEGER
        );
    ''')
    db.commit()
    return


def _putTrafficData(db, data):
    cur= db.cursor()
    try:
        origin= data['origin_coord']
        destination= data['destination_coord']
        recordTime= datetime.strptime(data['time'], '%Y%m%d%H%M%S')
        driveTime= data['rows'][0]['elements'][0]['duration_in_traffic']['value'] #in seconds

    except (KeyError):
        print('data missing for: %s - %s' % (data['origin_addresses'], data['destination_addresses']) )
        return

    cur.execute('INSERT OR IGNORE INTO Waypoint (lat, lon) VALUES (?,?)', origin )
    origin_id= cur.execute('SELECT id FROM Waypoint WHERE lat=? AND lon=?', origin).fetchone()[0]

    cur.execute('INSERT OR IGNORE INTO Waypoint (lat, lon) VALUES (?,?)', destination)
    destination_id= cur.execute('SELECT id FROM Waypoint WHERE lat=? AND lon=?', destination).fetchone()[0]

    cur.execute('''INSERT INTO TrafficData
        (recordTime, origin_id, destination_id, driveTime) VALUES (?,?,?,?)''',
        (recordTime, origin_id, destination_id, driveTime)
    )

    db.commit()
    return


def exportDatas_db(db, datas):
    for data in datas:
        _putTrafficData(db, data)
    return

def putFolderToDB(db, fileDir):
    for f in os.listdir(fileDir):
        filePath= fileDir+'/'+ f
        if f.endswith('.json'):
            try:
                fh= open(filePath)
                fh.close()
            except:
                print('%s cannot be opened' % filePath)
                continue

            with open(filePath) as fh:
                print('Inserting: %s' % f)
                _putTrafficData(db, json.load(fh))
    return


if __name__== '__main__':
    db= init_db(os.path.join(DIR, 'traffic_db.sqlite'))
    putFolderToDB(db, r'_RAW')
    db.close()
