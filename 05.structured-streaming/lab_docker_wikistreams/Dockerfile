FROM quay.io/jupyter/pyspark-notebook@sha256:80430186e2e97c5e0a9a3c117798d4007bc0a289b8a72cf3a9fc92f044b4c4f1
COPY --chown=jovyan:users requirements.txt /tmp/tp05-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/tp05-requirements.txt && mkdir -p /home/jovyan/.ivy2.5.2
