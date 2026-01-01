FROM apache/hive:4.2.0

RUN wget -P /opt/hive/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
COPY ./config/core-site.xml /opt/hive/conf/core-site.xml