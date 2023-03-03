from ossaudiodev import AFMT_QUERY
from flask import Flask
from flask_mongoengine import MongoEngine
from database.db import initialize_db,db
from database.models import TestTable
from mongoengine.connection import get_db, connect
from datetime import datetime,timedelta
from bson.json_util import dumps
from bson.objectid import ObjectId
app = Flask(__name__)

app.config.from_object("config")
initialize_db(app)

@app.route("/")
def start_func():
    return "first flask function"

db=get_db()
print(db)
print(db['vesseltripdata'])


# From a given trip, give me all the data for "Sensor X" over time
# From a given timespan, give me all the data for "Sensor X" over time
def get_sensor_data(sensor_name,from_time,to_time,trip_id=None):
    # y=db['vesseltripdata'].find({'tripId' :trip_id,'sensorName':sensor_name,'dateTime':{'$gt':from_time,'$lt':to_time}})
    if trip_id:
        sensor_data=db.vesseltripdata.aggregate([{"$match":{"dateTime": {"$gte": from_time,"$lte" :to_time},"tripId":trip_id,'sensorName':sensor_name}},{"$project": {"_id":0}}])
    else:
        sensor_data=db.vesseltripdata.aggregate([{"$match":{"dateTime": {"$gte": from_time,"$lte" :to_time},'sensorName':sensor_name}},{"$project": {"_id":0}}])
    print(sensor_data)
    print(type(sensor_data))
    print(dumps(list(sensor_data)))
trip_id ='63d4228e01afc1d6b6813e38'
sensor_name="GPS"
from_time=datetime.now()-timedelta(days=30)
to_time=datetime.now()
# get_sensor_data(sensor_name,from_time,to_time,trip_id)

# From a given trip, give the sum of "Sensor X" throughout the entire trip
def get_sensor_data_sum(sensor_name,trip_id):
    sensor_sum_data=db.vesseltripdata.aggregate([{"$match":{"tripId":trip_id,'sensorName':sensor_name}},{"$addFields": {"arraySize":{"$size":"$dataPoints"}}},{"$group": {"_id":"$sensorName","index0_sum":{"$sum":{"$arrayElemAt":["$dataPoints",0]}},"index1_sum":{"$sum":{"$arrayElemAt":["$dataPoints",1]}},"index2_sum":{"$sum":{"$arrayElemAt":["$dataPoints",2]}}}}])
    print(sensor_sum_data)
    print(type(sensor_sum_data))
    print(dumps(list(sensor_sum_data)))
# get_sensor_data_sum(sensor_name,trip_id)

# From a given trip, give the average of "Sensor X" throughout the entire trip
def get_sensor_data_avg(sensor_name,trip_id):
    sensor_sum_data=db.vesseltripdata.aggregate([{"$match":{"tripId":trip_id,'sensorName':sensor_name}},{"$addFields": {"arraySize":{"$size":"$dataPoints"}}},{"$group": {"_id":"$sensorName","index0_avg":{"$avg":{"$arrayElemAt":["$dataPoints",0]}},"index1_avg":{"$avg":{"$arrayElemAt":["$dataPoints",1]}},"index2_avg":{"$avg":{"$arrayElemAt":["$dataPoints",2]}}}}])
    print(sensor_sum_data)
    print(type(sensor_sum_data))
    print(dumps(list(sensor_sum_data)))
# get_sensor_data_avg(sensor_name,trip_id)

# From a given boat, give the average of “Sensor X” for each day between these dates
def get_boatsensor_data_avg(sensor_name,start_date,end_date): 
    boatsensor_sum_data=db.vesseltripdata.aggregate([{"$match":{"dateTime": {"$gte": from_time,"$lte" :to_time},'sensorName':sensor_name}},{"$addFields": {"arraySize":{"$size":"$dataPoints"}}},{"$group": {"_id":{"day":{"$dayOfMonth":"$dateTime"},"month":{"$month":"$dateTime"},"year":{"$year":"$dateTime"}},"index0_avg":{"$avg":{"$arrayElemAt":["$dataPoints",0]}},"index1_avg":{"$avg":{"$arrayElemAt":["$dataPoints",1]}},"index2_avg":{"$avg":{"$arrayElemAt":["$dataPoints",2]}}}}])
    print(boatsensor_sum_data)
    print(type(boatsensor_sum_data))
    print(dumps(list(boatsensor_sum_data)))
start_date=datetime.now()-timedelta(days=30)
end_date=datetime.now()
# get_boatsensor_data_avg(sensor_name,start_date,end_date)

# For a given trip, tell me which sensors were available
def get_trip_sensors(trip_id): 
    trip_sensors_data=db.vesseltripdata.aggregate([{"$match":{"tripId":trip_id}},{"$group": {"_id":"$tripId","sensors":{"$addToSet":"$sensorName"}}}])
    print(trip_sensors_data)
    print(type(trip_sensors_data))
    print(dumps(list(trip_sensors_data)))
# get_trip_sensors(trip_id)



def get_distance_travelled_intrip(trip_id,sensor_name):
    number_of_documents=len(list(db.vesseltripdata.find({"tripId":trip_id,"sensorName":sensor_name})))
    print(number_of_documents)
    math_pi = 3.141592653589793
    rad = 6371
    #for reference - l=[[lat1,lon1][lat2,lon2]],dlat=l[1][0]-l[0][0],dlon=l[1][1]-l[0][1],lat1=l[0][0],lat2=l[1][0]
    dlat_query={"$divide":[{"$multiply":[{"$subtract":[{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",1]},0]},{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",0]},0]}]},math_pi]},180.0]}
    dlon_query={"$divide":[{"$multiply":[{"$subtract":[{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",1]},1]},{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",0]},1]}]},math_pi]},180.0]}
    lat1_query={"$divide":[{"$multiply":[{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",0]},0]},math_pi]},180.0]}
    lat2_query={"$divide":[{"$multiply":[{"$arrayElemAt":[{"$arrayElemAt":["$groupedDatapoints",1]},0]},math_pi]},180.0]}
    aformula_query={"$add":[{"$pow":[{"$sin":{"$divide":["$dlat",2]}},2]},{"$multiply":[{"$pow":[{"$sin":{"$divide":["$dlon",2]}},2]},{"$cos":"$lat1"},{"$cos":"$lat2"}]}]}
    cformula_query={"$multiply":[{"$asin":{"$sqrt":"$aformula"}},2]}
    distance_query={"$multiply":["$cformula",rad]}
    trip_data = db.vesseltripdata.aggregate([{"$match":{"tripId":trip_id,"sensorName":sensor_name}},{"$setWindowFields":{"sortBy": {"dateTime":1},"output":{"indexNumber": {"$documentNumber": {}}}}},{"$addFields":{"indexList":["$indexNumber",{"$add":["$indexNumber",1]}]}},{"$unwind":"$indexList"},{"$group":{"_id":"$indexList","groupedDatapoints":{"$push":"$dataPoints"},"count":{"$sum":1}}},{"$addFields":{"dlat":dlat_query,"dlon":dlon_query,"lat1":lat1_query,"lat2":lat2_query}},{"$addFields":{"aformula":aformula_query}},{"$addFields":{"cformula":cformula_query}},{"$addFields":{"distance":distance_query,"total":"total"}},{"$group":{"_id":"$total","totalDistance":{"$sum":"$distance"}}}])
    print(dumps(list(trip_data)))
    return True
get_distance_travelled_intrip(trip_id,sensor_name)


#below formula is implemented on above mongodb query in function get_distance_travelled_intrip
#got below formula from this link - https://www.geeksforgeeks.org/haversine-formula-to-find-distance-between-two-points-on-a-sphere/
def haversine(lat1, lon1, lat2, lon2):
    import math
    # distance between latitudes
    # and longitudes
    dLat = (lat2 - lat1) * math.pi / 180.0
    dLon = (lon2 - lon1) * math.pi / 180.0
 
    # convert to radians
    lat1 = (lat1) * math.pi / 180.0
    lat2 = (lat2) * math.pi / 180.0
 
    # apply formulae
    a = (pow(math.sin(dLat / 2), 2) +
         pow(math.sin(dLon / 2), 2) *
             math.cos(lat1) * math.cos(lat2));
    rad = 6371
    c = 2 * math.asin(math.sqrt(a))
    print("dLat:",dLat,"dLon:",dLon,"lat1:",lat1,"lat2:",lat2,"aformula:",a,"cformula:",c,"distance:",rad*c)
    return rad * c

lat1=47.640438
lon1=-69.5660486
lat2=44.640438
lon2=-63.5660486

print(haversine(lat1, lon1, lat2, lon2))
if __name__=="__main__":
    app.run()