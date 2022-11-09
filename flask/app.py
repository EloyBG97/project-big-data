from flask import Flask, render_template, request
import folium

app = Flask(__name__)


@app.route('/')
def hello_world():
    m = folium.Map(location=[36.7201600, -4.4203400], zoom_start=13)

    def add_marker(m, location):
        folium.Marker(
            location=location,
            popup="Prueba de popup",
            icon=folium.Icon(color="blue", icon="bus", prefix='fa')
        ).add_to(m)
        return m

    m = add_marker(m, [36.7201600, -4.4203400])
    return render_template(
        "index.html",
        map=m._repr_html_()
    )


@app.route('/refresh', methods=["POST"])
def refresh():
    if request.method == "POST":
        m = folium.Map(location=[36.7201600, -4.4203400], zoom_start=13)
        print("Refresh")
        return render_template(
            "index.html",
            map=m._repr_html_()
        )


@app.route('/filter', methods = ["GET"])
def filter():
    # [ELOY] TODO: Desarrollar recurso /filter.
    # Conectar con Kafka
    # Filtrar el contenido de Kafka de acuerdo a los parametros recibidos
    # Mostrar la información obtenida en el mapa

    linea = request.args.get('linea', 1)
    sentido = request.args.get('sentido', 1)
    last_time = request.args.get('last-time')

    print(f"{linea} {sentido} {last_time}")
    m = folium.Map(location=[36.7201600, -4.4203400], zoom_start=13)
    return render_template(
        "index.html",
        map=m._repr_html_()
    )


if __name__ == '__main__':
    app.run()
