import sys
import time
import urllib.request

if len(sys.argv) != 1:
    print("Revise el programa. Algo no ha salido correctamente. ", file=sys.stderr)
    exit(-1)

urlToFetch = "https://datosabiertos.malaga.eu/recursos/transporte/EMT/EMTLineasUbicaciones/lineasyubicaciones.csv"
streamingDirectory = "csv/inlive/"
secondsToSleep = 60

while True:
    time_stamp = str(time.time())

    newFile = streamingDirectory + time_stamp + ".csv"
    urllib.request.urlretrieve(urlToFetch, newFile)
    print(newFile)
    time.sleep(secondsToSleep)
