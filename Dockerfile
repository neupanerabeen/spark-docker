from ubuntu:latest
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get -y install software-properties-common default-jre-headless openjdk-8-jdk ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN ssh-keygen -t rsa -f /root/.ssh/id_rsa -P ""
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && chmod 400 /root/.ssh/authorized_keys
COPY ./assets/ /opt/installed/


CMD ["tail", "-f", "/dev/null"]