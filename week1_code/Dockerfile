FROM python:3.8

WORKDIR app/

COPY ingest_data.py ingest_data.py
COPY requirements.txt requirements.txt

RUN  apt-get update \
  && apt-get install -y wget
RUN pip install -r requirements.txt