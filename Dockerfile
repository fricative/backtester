FROM python:3.6

WORKDIR /src
COPY . /src

RUN pip install -r /src/requirements.txt