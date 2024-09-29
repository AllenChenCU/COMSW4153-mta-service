FROM python:3.10-slim

COPY app /app

COPY requirements.txt /app

WORKDIR /app

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5001

ARG DBHOST
ENV DBHOST=$DBHOST
ARG DBUSER
ENV DBUSER=$DBUSER
ARG DBPASSWORD
ENV DBPASSWORD=$DBPASSWORD
ARG DBPORT
ENV DBPORT=$DBPORT
ARG DBNAME
ENV DBNAME=$DBNAME

CMD ["python3", "/app/main.py"]
