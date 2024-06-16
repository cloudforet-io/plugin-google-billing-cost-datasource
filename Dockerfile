FROM cloudforet/python-core:2.0

ENV PYTHONUNBUFFERED 1
ENV SPACEONE_PORT 50051
ENV SERVER_TYPE grpc
ENV PKG_DIR /tmp/pkg
ENV SRC_DIR /tmp/src

RUN apt update && apt upgrade -y

COPY pkg/*.txt ${PKG_DIR}/

RUN pip install --upgrade pip && \
    pip install --upgrade -r ${PKG_DIR}/pip_requirements.txt && \
    pip install --upgrade --pre spaceone-core spaceone-api spaceone-cost-analysis==2.0.dev120

COPY src ${SRC_DIR}
WORKDIR ${SRC_DIR}
RUN python3 setup.py install && rm -rf /tmp/*

EXPOSE ${SPACEONE_PORT}

ENTRYPOINT ["spaceone"]
CMD ["run", "plugin-server", "plugin"]
