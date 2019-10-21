FROM library/postgres:12
COPY *.sql /docker-entrypoint-initdb.d/
RUN apt-get update && apt-get upgrade -y && apt-get install -y postgresql-contrib