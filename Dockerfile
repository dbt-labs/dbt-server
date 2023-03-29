# python:3.8-slim-bullseye
ARG BASE_IMAGE=python@sha256:bb908c726535fc6787a69d4ef3cdb5ee90dc5edeae56da3181b2108539a5eb64
FROM $BASE_IMAGE

ARG DBT_PIP_FLAGS
ARG DBT_CORE_PACKAGE
ARG DBT_DATABASE_ADAPTER_PACKAGE
ARG DATADOG_PACKAGE

RUN apt-get -y update && apt-get -y upgrade && \
  apt-get -y update --fix-missing && \
  apt-get -y install && apt-get -y upgrade && \
  apt-get -y install software-properties-common && \
  apt-get -y install git libpq-dev openssh-client openssl && \
  apt-get -y autoremove && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY ./bash /usr/src/app/bash
# Copy celery config and serviced.
COPY ./configs/celeryd /etc/default/celeryd
COPY ./bash/celeryd.sh /etc/init.d/celeryd
# Create celery worker user.
RUN groupadd celery && useradd celery -g celery
# Install redis server.
RUN /usr/src/app/bash/ubuntu-setup-redis.sh

COPY requirements.txt /usr/src/app

RUN pip install                     \
    --no-cache-dir                  \
    --upgrade                       \
    -r requirements.txt             \
    ${DBT_PIP_FLAGS}                \
    ${DBT_CORE_PACKAGE}             \
    ${DBT_DATABASE_ADAPTER_PACKAGE} \
    ${DATADOG_PACKAGE}

RUN pip install --force-reinstall MarkupSafe==2.0.1 # TODO: find better fix for this

COPY ./dbt_server /usr/src/app/dbt_server
COPY ./dbt_worker /usr/src/app/dbt_worker

# TODO: Currently Celery worker user is "celery" instead of "root", dbt server 
# has piece of code to create working-dir directory. Bad thing is Celery worker 
# will try to create working-dir as well without write privilege and crashed 
# eventually. This is bad behavior, in short we change root directory mod to 
# 777, Celery worker can create working-dir as well. In long term we shouldn't 
# let Celery worker do this directory initialization job.
RUN chmod 777 /usr/src/app
