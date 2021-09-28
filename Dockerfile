# docker build -f Dockerfile . -t dbt-server

FROM python:3.8

COPY . /usr/src

WORKDIR /usr/src

RUN pip install -r requirements.txt

CMD uvicorn dbt_server.server:app --reload --host 0.0.0.0 --port 8580

EXPOSE 8580
