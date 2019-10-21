CREATE USER "sofia_user";
ALTER USER "sofia_user" WITH PASSWORD 'sofia_user';
CREATE DATABASE sofiadb WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.utf8' LC_CTYPE = 'en_US.utf8';
ALTER DATABASE sofiadb OWNER TO "sofia_user";
GRANT ALL PRIVILEGES ON DATABASE sofiadb TO "sofia_user";