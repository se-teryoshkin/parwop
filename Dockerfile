FROM python:3.10

WORKDIR /
COPY parwop/requirements.txt /requirements.txt

RUN pip install -r /requirements.txt
