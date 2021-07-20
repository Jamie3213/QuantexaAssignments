# Ubuntu base image
FROM ubuntu:18.04

# Install initial dependencies
RUN apt-get update && \
    apt-get -y install git curl wget unzip zip

# Set default charset
ENV LC_ALL="C.UTF-8"
ENV LANG="$LC_ALL"

# Install OpenJDK 11 and add environment variables
RUN apt-get -y install openjdk-11-jdk

RUN JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
ENV PATH="$PATH:/usr/lib/jvm/java-11-openjdk-arm64/bin"

# Install Gradle and configure environment variables
RUN wget https://services.gradle.org/distributions/gradle-7.1.1-bin.zip && \
    unzip -d /opt/gradle gradle-7.1.1-bin.zip

ENV GRADLE_HOME="/opt/gradle"
ENV PATH="$PATH:/opt/gradle/gradle-7.1.1/bin"

# Install Scala 2.12.14
RUN wget https://downloads.lightbend.com/scala/2.12.14/scala-2.12.14.deb && \
    dpkg -i scala-2.12.14.deb && \
    apt-get install scala

# Install Spark 3.1.2 and configure environment variables
RUN wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar xvzf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /opt/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

ENV SPARK_HOME="/opt/spark"
ENV PATH="$PATH:/opt/spark/bin:/opt/spark/sbin"
