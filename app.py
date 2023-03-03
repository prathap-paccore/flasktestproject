from flask import Flask
app = Flask(__name__)
from mongoengine.connection import connect,get_db
connect('goe', host='mongodb://localhost:27017/goe')
db=get_db()
print(db.vesseltripdata.find({}))
for x in db.vesseltripdata.find({}):
    print(x)
@app.route('/')
def hello_world():
    return 'Hello world!'