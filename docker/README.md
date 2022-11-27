Para ejecutar todo:

```sh
# `build` si cambiamos algo en los Dockerfile
docker-compose build

docker-compose up
```

<hr/>

Ejecutar solo spark streaming (mandando los datos al servidor kafka):

```sh
docker-compose up spark
```

En la carpeta kafka hay scripts para probar el servidor kafka (que está en el puerto 9092).

<hr/>

Para ejecutar spark mostrando por consola, comentamos la sección `depends_on`, añadimos el código y lanzamos docker-compose:

```docker-compose
  spark:
    container_name: spark-streaming
    build: ./spark
    volumes:
      - ./spark:/code
      - ./datos-abiertos/data/:/code/data
    # depends_on:
    #   - kafka
    environment:
      - dataRoute=../datos-abiertos/data/
      - kafkaRoute=localhost:9092
    network_mode: host
```

```python
query = values \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
```

```sh
docker-compose up spark
```