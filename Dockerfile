FROM python:3.8
WORKDIR /app

RUN git clone https://github.com/AusSRC/SoFiAX.git &&\
    cd SoFiAX &&\
    python3 setup.py install

ENTRYPOINT [ "sofiax" ]