FROM python:3.11

RUN mkdir -p /app
WORKDIR /app
ADD config.yaml /app/
RUN git clone https://github.com/heiparta/nordpool2influxdb.git

RUN pip install -r nordpool2influxdb/requirements.txt


CMD ["python", "nordpool2influxdb/src/nordpool2influxdb/nordpool2influxdb.py", "config.yaml"]
