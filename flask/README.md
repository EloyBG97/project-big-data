# Instrucciones de ejecución
1. Lanzar docker con kafka: 
```
cd kafka
docker-compose -f docker-compose-expose.yml up
```
2. Ejecutar flask y abrir localhost:5000/
3. Ejecutar spark.py
4. Ejecutar fetchFileByInterval.py


# Enlaces
https://towardsdatascience.com/kafka-docker-python-408baf0e1088
https://medium.com/geekculture/streaming-model-inference-using-flask-and-kafka-3476d9ff5ca5
https://www.youtube.com/watch?v=hfi_ALPlsOQ
https://github.com/wurstmeister/kafka-docker
https://medium.com/geekculture/integrate-kafka-with-pyspark-f77a49491087