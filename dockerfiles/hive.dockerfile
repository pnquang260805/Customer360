FROM apache/hive:3.1.3

USER root
RUN apt update -y && apt install wget -y
ENV HIVE_VERSION=3.1.3

RUN wget -P /opt/hive/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.4/postgresql-42.5.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chown -R 1000:1000 /opt/hive/lib/

COPY ./bash/hive-entrypoint.sh .
RUN chmod +x ./hive-entrypoint.sh
USER 1000
COPY ./config/core-site.xml /opt/hive/conf/core-site.xml
COPY ./config/hive-site.xml /opt/hive/conf/hive-site.xml

ENTRYPOINT [ "./hive-entrypoint.sh" ]