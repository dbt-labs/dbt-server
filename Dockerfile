# python:3.8-slim-bullseye
ARG BASE_IMAGE=python@sha256:bb908c726535fc6787a69d4ef3cdb5ee90dc5edeae56da3181b2108539a5eb64
FROM $BASE_IMAGE

ARG DBT_CORE_PACKAGE
ARG DBT_DATABASE_ADAPTER_PACKAGE

RUN apt-get -y update && apt-get -y upgrade && \
  apt-get -y update --fix-missing && \
  apt-get -y install && apt-get -y upgrade && \
  apt-get -y install software-properties-common && \
  apt-get -y install git libpq-dev openssh-client openssl && \
  apt-get -y autoremove && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app

RUN pip install --no-cache-dir --upgrade -r requirements.txt $DBT_CORE_PACKAGE $DBT_DATABASE_ADAPTER_PACKAGE

COPY ./dbt_server /usr/src/app/dbt_server
