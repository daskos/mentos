FROM python:3.5.2-alpine

ADD . /mentos
WORKDIR /mentos
RUN pip install .