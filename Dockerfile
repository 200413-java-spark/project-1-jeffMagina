FROM postgres
ENV POSTGRES_DB jeffMaginaProject1
ENV POSTGRES_USER jeffMagina
ENV POSTGRES_PASSWORD jeffMagina
ADD schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432
