from json import loads
from flask_socketio import SocketIO
from flask import Flask, render_template
import folium
from kafka import KafkaConsumer
import requests
import os
import threading

TOPIC_TEST = "topic_test"
TOPIC_FILTER = "topic_filter"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")
thread = None
kafka_route = os.environ.get('kafkaRoute')


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


def get_messages_test(arg):
    print('Iniciado Thread')
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_route],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=[TOPIC_TEST])
    m = get_empty_map()
    update_map(m)

    timestamp = None

    # Poll permite obtener los datos recibidos en los ultimos x segundos
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        seconds = 5
        records = consumer.poll(seconds * 1000)

        if records != {}:
            record_list = []

            for tp, consumer_records in records.items():
                for consumer_record in consumer_records:
                    print(consumer_record.value)
                    record_list.append(consumer_record.value)

            for item in record_list:
                if timestamp != item['timestamp']:
                    timestamp = item['timestamp']
                    m = get_empty_map()
                m = add_marker(m, [item['lat'], item['lon']], f"Bus:{item['codBus']} | Linea: {item['codLinea']}")

            update_map(m)

    print('Parado Thread')

def get_messages_filter(arg):
    print('Iniciado Thread')
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_route],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=[TOPIC_FILTER])
    m = get_empty_map()
    update_map(m)

    timestamp = None

    # Poll permite obtener los datos recibidos en los ultimos x segundos
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        seconds = 5
        records = consumer.poll(seconds * 1000)

        if records != {}:
            record_list = []

            for tp, consumer_records in records.items():
                for consumer_record in consumer_records:
                    print(consumer_record.value)
                    record_list.append(consumer_record.value)

            for item in record_list:
                m = add_marker(m, [item['lat'], item['lon']], f"Bus:{item['codBus']} | Linea: {item['codLinea']} | Sentido: {item['sentido']} | Actualizacion: {item['last_update']}")

            update_map(m)

    print('Parado Thread')



@app.route('/')
def inicio():
    global thread
    if thread is not None:
        thread.do_run = False
        thread.join()

    thread = threading.Thread(target=get_messages_test, args=("task",))
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )


@app.route('/filter', methods=["GET"])
def filter():
    # [ELOY] TODO: Desarrollar recurso /filter.
    # Conectar con Kafka
    # Filtrar el contenido de Kafka de acuerdo a los parametros recibidos
    # Mostrar la información obtenida en el mapa

    global thread
    if thread is not None:
        thread.do_run = False
        thread.join()

    thread = threading.Thread(target=get_messages_filter, args=("task",))
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )



if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0')
