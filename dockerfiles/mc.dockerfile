FROM minio/mc

WORKDIR /mymc
COPY ./policy/* .
COPY ./bash/mc.sh ./mymc.sh
RUN chmod +x ./mymc.sh

ENTRYPOINT ["/bin/sh", "./mymc.sh"]