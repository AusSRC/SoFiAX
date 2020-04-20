FROM library/postgres:12.2
RUN apt-get update && apt-get upgrade -y && apt-get install -y postgresql-contrib vim
RUN apt-get install -y postgis
RUN apt-get install -y postgresql-pgsphere
COPY *.sql /docker-entrypoint-initdb.d/
