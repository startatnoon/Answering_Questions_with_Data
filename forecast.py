import requests
import sqlite3 as lite
import datetime

cities = { "Atlanta": '33.762909,-84.422675',
            "Austin": '30.303936,-97.754355',
            "Boston": '42.331960,-71.020173',
            "Chicago": '41.837551,-87.681844',
            "Miami": '25.775163,-80.208615',
            "Minneapolis": '44.963324,-93.268320'
        }

api_key = 'ddc7311f08062c229b8eb809d3ec4c0d'
#start_desired = '2015-01-01T00:00:00'
start_time = datetime.datetime(2015,1,1,0,0,0)
end_time = start_time + datetime.timedelta(days=30)
url_pattern = 'https://api.forecast.io/forecast/' + api_key + '/' 
#r = requests.get(url_pattern)
print url_pattern

con = lite.connect('weather.db')
cur = con.cursor()

cities.keys()
with con:
	cur.execute('CREATE TABLE daily_temp (day_of_reading INT, Atlanta REAL, Austin REAL, Boston REAL, Chicago REAL, Miami REAL, Minneapolis REAL);')

query_date = start_time

with con:
    while query_date < end_time:
        cur.execute("INSERT INTO daily_temp(day_of_reading) VALUES (?)", (int(query_date.strftime('%s')),))
        query_date += datetime.timedelta(days=1)


for k,v in cities.iteritems():
    query_date = start_time #set value each time through the loop of cities
    while query_date < end_time:
        #query for the value
        r = requests.get(url_pattern + v + ',' +  query_date.strftime('%Y-%m-%dT00:00:00'))

        with con:
            #insert the temperature max to the database
            cur.execute('UPDATE daily_temp SET ' + k + ' = ' + str(r.json()['daily']['data'][0]['temperatureMax']) + ' WHERE day_of_reading = ' + query_date.strftime('%s'))

        #increment query_date to the next day for next operation of loop
        query_date += datetime.timedelta(days=1) #increment query_date to the next day


con.close()