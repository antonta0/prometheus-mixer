FROM alpine:latest

COPY prometheus-mixer /bin/prometheus-mixer

USER       nobody
EXPOSE     9900
WORKDIR    /
ENTRYPOINT [ "/bin/prometheus-mixer" ]
