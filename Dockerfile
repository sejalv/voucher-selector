FROM apache/airflow:2.3.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN python -m pip install --user --upgrade pip
COPY requirements.txt /usr/local/airflow/
RUN python -m pip install --user -r requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt
