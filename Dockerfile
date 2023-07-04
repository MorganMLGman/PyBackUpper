FROM python:3-alpine

WORKDIR /usr/src/app

COPY ./src/requirements.txt ./

RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/main" >> /etc/apk/repositories && \
    echo "http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories && \
    echo "http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
    apk update

RUN pip install --no-cache-dir --upgrade pip && \ 
    pip install --no-cache-dir --upgrade setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt && \
    apk add --no-cache tar pigz pixz && \
    mkdir -p /source /target /logs

COPY ./src ./

ENTRYPOINT [ "python", "-u", "./main.py" ]