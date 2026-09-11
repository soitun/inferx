# syntax=docker/dockerfile:1

FROM python:3.10-slim-bookworm

WORKDIR /

RUN apt-get -y update \
    && apt-get install -y --no-install-recommends libpq-dev gcc bash nginx curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

COPY nginx.conf /etc/nginx/sites-available/default

CMD service nginx start && gunicorn -w 4 -b 0.0.0.0:1250 app:app
# CMD service nginx start && python3 ./app.py
# CMD python3 ./app.py
