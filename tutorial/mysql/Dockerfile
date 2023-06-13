# NOTE name need to be Dockerfile
FROM mysql:latest
COPY ./databse_students.sql /docker-entrypoint-initdb.d/
COPY ./databse_raw_events.sql /docker-entrypoint-initdb.d/