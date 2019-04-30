
FROM python:3

COPY . /var/app/registry

WORKDIR /var/app/registry

RUN pip install -r requirements.txt
