FROM semantive/spark
MAINTAINER Semantive "https://github.com/semantive"

RUN apt-get -y update \
 && apt-get -y upgrade \
 && apt-get install -y openssh-server \
 && rm -rf /var/lib/apt/lists/*

# Generated ssh keys
RUN ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
RUN cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
RUN mkdir /var/run/sshd

COPY conf/core-site.xml /opt/hadoop-2.7.3/etc/hadoop/
COPY conf/hdfs-site.xml /opt/hadoop-2.7.3/etc/hadoop/
COPY conf/mapred-site.xml /opt/hadoop-2.7.3/etc/hadoop/
RUN echo "#!/bin/bash" > /bin/hadoop-runner.sh
RUN echo "set -m" >> /bin/hadoop-runner.sh
RUN echo "/usr/sbin/sshd -D &" >> /bin/hadoop-runner.sh
RUN echo "yes n | hdfs namenode -format" >> /bin/hadoop-runner.sh
RUN echo "/opt/hadoop-2.7.3/sbin/start-all.sh" >> /bin/hadoop-runner.sh
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /opt/hadoop-2.7.3/etc/hadoop/hadoop-env.sh
RUN echo "ip addr" >> /bin/hadoop-runner.sh
RUN echo "fg" >> /bin/hadoop-runner.sh
RUN chmod a+x /bin/hadoop-runner.sh

COPY ssh_config /etc/ssh/ssh_config

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
