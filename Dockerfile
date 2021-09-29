# docker build -f Dockerfile . -t dbt-server

FROM python:3.8

COPY . /usr/src

WORKDIR /usr/src

RUN pip install -r requirements.txt
