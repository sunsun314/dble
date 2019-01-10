From centos:centos7
RUN yum install -y wget java

RUN wget -P /opt ftp://ftp:ftp@10.186.18.20/actiontech-dble/qa/9.9.9.9/actiontech-dble.tar.gz && \
    tar zxvf /opt/actiontech-dble.tar.gz -C /opt && \


VOLUME /opt/dble

CMD ["/opt/dble/bin/docker_init_start.sh", "daemon off;"]