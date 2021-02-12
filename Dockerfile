FROM python:3.8.2-alpine3.11

RUN apk update \
    && apk add git python3-dev build-base \
    && pip3 install wheel

COPY . /app
WORKDIR /app

# shellcheck disable=DL3013
RUN pip3 install -e . \
    && apk del git python3-dev build-base

CMD kopf run --standalone ./src/rating/manager/main.py
