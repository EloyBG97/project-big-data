FROM apache/spark-py:v3.2.2


#FROM python:3.10-slim

#RUN pip install --upgrade pip
USER root

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["python3","./main.py"]
