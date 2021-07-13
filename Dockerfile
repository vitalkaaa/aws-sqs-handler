FROM python:3.9-alpine
RUN apk add --update --no-cache --virtual .tmp-build-deps gcc libc-dev linux-headers build-base
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt
WORKDIR /app
