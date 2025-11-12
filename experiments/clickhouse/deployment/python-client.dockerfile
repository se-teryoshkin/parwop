FROM python:3.10

RUN pip install clickhouse_driver

CMD ["bash", "-c", "while true; do sleep 5; done"]
