import time
from json import loads
from flask_socketio import SocketIO
from flask import Flask, render_template
import folium
from kafka import KafkaConsumer

import threading

TOPIC = "topic_test"
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")


@socketio.on('connect', namespace='/test')
def handle_connect():
    print('Client connected')


def get_empty_map():
    return folium.Map(location=[36.7201600, -4.4203400], zoom_start=13)


def update_map(m):
    socketio.emit('mapa', {'data': m._repr_html_()}, broadcast=True)


def add_marker(m, location, text):
    folium.Marker(
        location=location,
        popup=text,
        icon=folium.Icon(color="blue", icon="bus", prefix='fa')
    ).add_to(m)
    return m


def get_messages():
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=[TOPIC])
    m = get_empty_map()
    update_map(m)



    while True:
        seconds = 3  # Es necesario usar poll para evitar que el mapa parpadee
        records = consumer.poll(seconds * 1000)

        if records != {}:
            record_list = []
            m = get_empty_map()
            for tp, consumer_records in records.items():
                for consumer_record in consumer_records:
                    record_list.append(consumer_record.value)

            for item in record_list:
                m = add_marker(m, [item['lat'], item['lon']], f"Bus:{item['codBus']} | Linea: {item['codLinea']}")

            update_map(m)
        else:
            time.sleep(seconds)


@app.route('/')
def inicio():
    thread = threading.Thread(target=get_messages)
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0')
