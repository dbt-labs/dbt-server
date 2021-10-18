ARG BASE_IMAGE=python:3.8-slim-bullseye

FROM $BASE_IMAGE

ARG DBT_CORE_VERSION
ARG DBT_DATABASE_ADAPTER_PACKAGE

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app

RUN pip install --no-cache-dir --upgrade -r requirements.txt dbt-core==$DBT_CORE_VERSION $DBT_DATABASE_ADAPTER_PACKAGE

COPY ./dbt_server /usr/src/app/dbt_server
