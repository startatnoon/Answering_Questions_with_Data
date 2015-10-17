import requests
r = requests.get('http://www.citibikenyc.com/stations/json')
r.text
r.json()
r.json().keys()
r.json()['stationBeanList']
len(r.json()['stationBeanList'])

key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

r.json()['stationBeanList'][0]

from pandas.io.json import json_normalize

df = json_normalize(r.json()['stationBeanList'])

import matplotlib.pyplot as plt
import pandas as pd

#df['availableBikes'].hist()
#plt.show()

#df['totalDocks'].hist()
#plt.show()
df.totalDocks.describe()

condition = (df['statusValue'] == 'In Service')
df[condition]['totalDocks'].mean()

df['totalDocks'].median()
df[df['statusValue'] == 'In Service']['totalDocks'].median()

#make a SQL table
import sqlite3 as lite

con = lite.connect('citi_bike.db') #does it automatically make it if it doesn't exist?
cur = con.cursor()

with con:
    cur.execute('DROP TABLE IF EXISTS citibike_reference')

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')


#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))


#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids] 

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
#with con:
#    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")


# a package with datetime objects
import time

# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 

import collections


#take the string and parse it into a Python datetime object
exec_time =parse(r.json()['executionTime'])

with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),)) #%s is formatting seconds

id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")



#credit to http://stackoverflow.com/questions/16879170/how-do-i-get-python-code-to-loop-every-x-mins-between-the-times-y-and-z
from sched import scheduler
from time import time, sleep

s = scheduler(time, sleep)

def run_periodically(start, end, interval, func):
    event_time = start
    while event_time < end:
        s.enterabs(event_time, 0, func, ())
        event_time += interval
    s.run()

if __name__ == '__main__':  #what is this doing?

    def update_data():
                exec_time =parse(r.json()['executionTime'])

        with con:
            cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),)) #%s is formatting seconds

        id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

        #loop through the stations in the station list
        for station in r.json()['stationBeanList']:
            id_bikes[station['id']] = station['availableBikes']

        #iterate through the defaultdict to update the values in the database
        with con:
            for k, v in id_bikes.iteritems():
                cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
   

    run_periodically(time()+60, time()+(60*60), 60, update_data)  
    #now I believe this will run in my terminal for 1 hour, is there a way to have it run in the background?

