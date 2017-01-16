FROM python:3.5.2-alpine

ADD . /opt/mentos
RUN pip --no-cache-dir install /opt/mentos \