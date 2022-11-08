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


if __name__ == '__main__':
    app.run()
