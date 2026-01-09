FROM spark:4.0.1-scala2.13-java21-ubuntu

USER root
RUN apt-get update -y && apt-get install curl -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# ENV: MODE -> define master role or worker role
COPY ./bash/spark-entrypoint.sh .
RUN chmod +x ./spark-entrypoint.sh

USER spark

CMD [ "./spark-entrypoint.sh" ]