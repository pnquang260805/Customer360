FROM jupyter/base-notebook:x86_64-python-3.11.6

USER root
RUN apt-get update && apt-get install openjdk-11-jdk -y

USER jovyan
RUN pip install pyspark==3.5.0