FROM python:3.5.2-alpine

ADD . /opt/malefico
RUN pip --no-cache-dir install /opt/malefico \