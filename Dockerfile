FROM europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/r-base:latest

WORKDIR /app

COPY R/ /app/R/

CMD ["Rscript", "R/main.R"]
