FROM python:3.8-buster
ARG DBT_DATABASE_ADAPTER_PACKAGE

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN pip install -r requirements.txt ${DBT_DATABASE_ADAPTER_PACKAGE}
