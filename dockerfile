FROM apache/airflow

COPY requirements.txt /tmp/requirements.txt

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    vim \
    sendmail \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install -r /tmp/requirements.txt
