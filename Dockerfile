FROM python:3-slim

WORKDIR /usr/src/app

COPY ./src/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \ 
    pip install --no-cache-dir --upgrade setuptools wheel \
    pip install --no-cache-dir -r requirements.txt

COPY ./src ./

RUN mkdir -p /source /target /logs

ENTRYPOINT [ "python", "-u", "./main.py" ]