FROM eclipse-temurin:11-jre

ENV HADOOP_VERSION=3.4.2
ENV HIVE_VERSION=3.1.3

RUN apt-get update && apt-get install -y curl --no-install-recommends

# Download and extract the Hadoop binary package.
RUN curl https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar xvz -C /opt/ && \
# ln: tạo link giữa 2 folder
    ln -s /opt/hadoop-$HADOOP_VERSION /opt/hadoop && \ 
    rm -r /opt/hadoop/share/doc
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws* /opt/hadoop/share/hadoop/common/lib/ && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk* /opt/hadoop/share/hadoop/common/lib/

ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="/opt/spark/bin:/opt/hadoop/bin:${PATH}"

RUN curl https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}-bin.tar.gz \
    | tar xvz -C /opt/ 

RUN ln -s /opt/apache-hive-metastore-${HIVE_VERSION}-bin /opt/hive-metastore && \
    ln -s /opt/hadoop/share/hadoop/tools/lib/bundle-* /opt/hive-metastore/lib/ || true \
    ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-* /opt/hive-metastore/lib/ || true 

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hadoop/share/hadoop/common/lib/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/hive-metastore/lib
ADD https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}.jar /opt/hive-metastore/lib
ADD https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar /opt/hive-metastore/lib

RUN rm /opt/hive-metastore/lib/guava-19.0.jar && \
    cp /opt/hadoop/share/hadoop/common/lib/guava-*.jar /opt/hive-metastore/lib/

COPY ./bash/hive-entrypoint.sh /opt/hive-metastore
ENTRYPOINT [ "/opt/hive-metastore/hive-entrypoint.sh" ]