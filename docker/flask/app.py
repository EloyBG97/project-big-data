from json import loads
from flask_socketio import SocketIO
from flask import Flask, render_template
import folium
from kafka import KafkaConsumer
import json
import os
import threading
import csv


def csv_to_json(csvFilePath):
    jsonArray = []

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        # load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            # add this python dict to json array
            jsonArray.append(row)
    return jsonArray


TOPIC_ALL = "topic_all"
TOPIC_FILTER = "topic_filter"
TOPIC_RADIO = "topic_radio"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")
thread = None
kafka_route = os.environ.get('kafkaRoute')

with open("../config.json", 'r') as f:
    filters = json.load(f)


@socketio.on('connect', namespace='/test')
def handle_connect():
    print('Client connected')


def get_empty_map():
    return folium.Map(location=[36.7201600, -4.4203400], zoom_start=13)


def update_map(m):
    socketio.emit('mapa', {'data': m._repr_html_()}, broadcast=True)


def add_marker(m, location, text, color="blue", icon="bus"):
    folium.Marker(
        location=location,
        popup=text,
        icon=folium.Icon(color=color, icon=icon, prefix='fa')
    ).add_to(m)
    return m


def get_messages_all(arg):
    print('Iniciado Thread')
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_route],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=[TOPIC_ALL])
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
                    record_list.append(consumer_record.value)

            for item in record_list:
                if timestamp != item['timestamp']:
                    timestamp = item['timestamp']
                    m = get_empty_map()
                m = add_marker(m, [item['lat'], item['lon']], f"Bus:{item['codBus']} | Linea: {item['codLinea']}")

            update_map(m)

    print('Parado Thread')

marker_colors = [
    'red',
    'blue',
    'gray',
    'darkred',
    'lightred',
    'orange',
    'beige',
    'green',
    'darkgreen',
    'lightgreen',
    'darkblue',
    'lightblue',
    'purple',
    'darkpurple',
    'pink',
    'cadetblue',
    'lightgray',
    'black'
]

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

    # Poll permite obtener los datos recibidos en los ultimos x segundos
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        seconds = 5
        records = consumer.poll(seconds * 1000)

        if records != {}:
            record_list = []

            for tp, consumer_records in records.items():
                for consumer_record in consumer_records:
                    record_list.append(consumer_record.value)

            for item in record_list:
                m = add_marker(m, [item['lat'], item['lon']],
                               f"Bus:{item['codBus']} | Linea: {item['codLinea']} | Sentido: {item['sentido']} | Actualizacion: {item['last_update']}",
                               color=marker_colors[int(item['codBus'])%len(marker_colors)])

            update_map(m)

    print('Parado Thread')


def mostrar_paradas(m):
    listado = csv_to_json('paradas.csv')
    parada1 = filters["4"]["paradaInicio"]
    parada2 = filters["4"]["paradaFin"]

    for parada in listado:
        color = "orange"
        if parada1 == parada['codParada']:
            color = "green"
        elif parada2 == parada['codParada']:
            color = "red"

        m = add_circle(m, [parada['lat'], parada['lon']],
                       f"Nombre:{parada['nombreParada']} | Código: {parada['codParada']}",
                       color=color)
    return m


def add_circle(m, location, text, color="blue"):
    folium.CircleMarker(
        fill_opacity=1,
        radius=5,
        color=color,
        fill_color=color,
        location=location,
        popup=text
    ).add_to(m)
    return m


def get_messages_radio(arg):
    print('Iniciado Thread')
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_route],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=[TOPIC_RADIO])
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
                    record_list.append(consumer_record.value)

            for item in record_list:
                if timestamp != item['timestamp']:
                    timestamp = item['timestamp']
                    m = get_empty_map()
                    folium.Circle([filters["3"]['latitude'], filters["3"]['longitude']],
                                  radius=filters["3"]['radio']).add_to(m)
                m = add_marker(m, [item['lat'], item['lon']],
                               f"Bus:{item['codBus']} | Lineaa: {item['codLinea']} | Sentido: {item['sentido']}")

            update_map(m)

    print('Parado Thread')


def get_messages_ej4(arg):
    print('Iniciado Thread')
    consumer = KafkaConsumer(
        bootstrap_servers=[kafka_route],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-id',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    consumer.subscribe(topics=['topic4'])
    m = get_empty_map()
    update_map(m)

    timestamp = None
    distanciaMinima = float('inf')
    listado = []

    # Poll permite obtener los datos recibidos en los ultimos x segundos
    t = threading.current_thread()
    while getattr(t, "do_run", True):
        seconds = 5
        records = consumer.poll(seconds * 1000)

        if records != {}:
            record_list = []

            for tp, consumer_records in records.items():
                for consumer_record in consumer_records:
                    record_list.append(consumer_record.value)

            for item in record_list:
                if timestamp != item['timestamp']:
                    timestamp = item['timestamp']
                    m = get_empty_map()
                    distanciaMinima = float('inf')
                    listado = []
                    m = mostrar_paradas(m)

                listado.append(item)
                if distanciaMinima > item['distanciaParadas']:
                    distanciaMinima = item['distanciaParadas']
                for bus in listado:
                    color = "red" if distanciaMinima == bus['distanciaParadas'] else "blue"
                    m = add_marker(m, [bus['lat'], bus['lon']],
                                   f"Bus:{bus['codBus']} | Linea: {bus['codLinea']} | Paradas restantes: {bus['distanciaParadas']}",
                                   color=color)

            update_map(m)

    print('Parado Thread')


@app.route('/ej4')
def ej4():
    global thread
    if thread is not None:
        thread.do_run = False
        thread.join()

    thread = threading.Thread(target=get_messages_ej4, args=("task",))
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )


@app.route('/')
def inicio():
    global thread
    if thread is not None:
        thread.do_run = False
        thread.join()

    thread = threading.Thread(target=get_messages_all, args=("task",))
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )


@app.route('/filter', methods=["GET"])
def filter():
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


@app.route('/radio', methods=["GET"])
def radio():
    # Conectar con Kafka
    # Mostrar los autobuses de servicio en un radio a partir de un punto dado
    global thread
    if thread is not None:
        thread.do_run = False
        thread.join()

    thread = threading.Thread(target=get_messages_radio, args=("task",))
    thread.daemon = True
    thread.start()

    return render_template(
        "index.html"
    )


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0')
