FROM library/postgres:12
RUN apt-get update && apt-get upgrade -y && apt-get install -y postgresql-contrib vim
RUN apt-get install -y postgis
COPY *.sql /docker-entrypoint-initdb.d/
