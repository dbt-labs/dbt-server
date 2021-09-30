# docker build -f Dockerfile . -t dbt-server

FROM python:3.8

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN pip install -r requirements.txt
