from sanic import Sanic, response, Request, Websocket
import paho.mqtt.client as mqtt

import json

from datetime import datetime

import sqlite3

from models import MqttORM


app = Sanic(__name__)


try:
    conn = sqlite3.connect('node-red-mqtt.db')
    print('Opened database successfully')

    conn.execute("""
        CREATE TABLE IF NOT EXISTS mqttData(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload TEXT NOTL NULL,
        time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP);""")
    print('Table created successfully')
    conn.close()

except AttributeError:
    print('Mqtt Data is not created')

    

def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))

    client.subscribe('#')


def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

    conn = sqlite3.connect('node-red-mqtt.db')
    sql = 'INSERT INTO mqttData (topic, payload) VALUES (?, ?)'
    val = (msg.topic, msg.payload)
    conn.execute(sql, val)
    conn.commit()

    try:
        data = {
            'topic': str(msg.topic),
            'payload': str(msg.payload),
            'time': str(datetime.now())
        }
        with open('mqtt_data.json', 'w') as outfile:
            json.dump(data, outfile)


    except TypeError:
        print('Something went wrong')



client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message


client.connect('127.0.0.1', 1883, 60)


@app.listener('after_server_start')
async def listener(app, loop):
    client.loop_start()
    print('listener_7')


@app.route('/')
async def handler(request):
    conn = sqlite3.connect('node-red-mqtt.db')
    sql = 'SELECT * FROM mqttData'
    data = conn.execute(sql)
    conn.commit()
    item = list(data)
    print(str(item))
    return response.json(str(item))


@app.websocket("/feed")
async def feed(request: Request, ws: Websocket):
    while True:
        conn = sqlite3.connect('node-red-mqtt.db')
        sql = 'SELECT * FROM mqttData'
        data = conn.execute(sql)
        conn.commit()
        item = list(data)
        print('Sending: ' + item)
        await ws.send(item)
        item = await ws.recv()


if __name__ == '__main__':
    app.run()