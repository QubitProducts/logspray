FROM scratch

COPY ./logs /opt/logspray/bin
WORKDIR  /opt/logspray/bin

ENTRYPOINT ["/opt/logspray/bin/logs"]
