ARG IMAGE_NAME

FROM ${IMAGE_NAME}

COPY ./config/mongo-keyfile /opt/keyfile/mongo-keyfile
RUN chmod 400 /opt/keyfile/mongo-keyfile && chown 999:999 /opt/keyfile/mongo-keyfile

CMD [ "mongod", "--replSet", "rs0", "--bind_ip_all", "--keyFile", "/opt/keyfile/mongo-keyfile" ]