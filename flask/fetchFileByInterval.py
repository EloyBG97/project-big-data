import time
import urllib.request

urlToFetch = "https://datosabiertos.malaga.eu/recursos/transporte/EMT/EMTLineasUbicaciones/lineasyubicaciones.csv"
streamingDirectory = "data"
secondsToSleep = 60


def save_data(urlToFetch, streamingDirectory, secondsToSleep):
    while True:
        time_stamp = str(time.time())

        newFile = streamingDirectory + "/" + time_stamp + ".txt"
        urllib.request.urlretrieve(urlToFetch, newFile)

        time.sleep(secondsToSleep)


if __name__ == '__main__':
    save_data(urlToFetch, streamingDirectory, secondsToSleep)
