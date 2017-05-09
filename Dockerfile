FROM scratch

COPY ./logspray-server /opt/logspray/bin
COPY ./logspray-reader /opt/logspray/bin
WORKDIR  /opt/logspray/bin

ENTRYPOINT ["/opt/logspray/bin/logspray-server"]
