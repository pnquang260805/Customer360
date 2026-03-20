FROM spark:3.5.0-scala2.12-java17-r-ubuntu
USER root
RUN apt-get update -y && apt-get install curl -y && apt-get clean && rm -rf /var/lib/apt/lists/*

# ENV: MODE -> define master role or worker role
COPY ./bash/spark-entrypoint.sh ./
RUN chmod +x ./spark-entrypoint.sh

RUN apt update -y && \
    apt install software-properties-common -y && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt update && \
    apt install python3.11 -y && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2

USER spark

CMD [ "./spark-entrypoint.sh" ]