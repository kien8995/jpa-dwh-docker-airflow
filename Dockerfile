FROM python:3.7-slim-stretch
LABEL maintainer="kientd.aits"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Postgres
ENV POSTGRES_HOST postgres
ENV POSTGRES_PORT 5432
ENV POSTGRES_USER airflow
ENV POSTGRES_PASSWORD airflow
ENV POSTGRES_DB airflow

# Airflow
ARG AIRFLOW_VERSION=1.10.5
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Airflow core config
ENV AIRFLOW__CORE__LOAD_EXAMPLES False
ENV AIRFLOW__CORE__DAGS_FOLDER ${AIRFLOW_USER_HOME}/dags
ENV AIRFLOW__CORE__LOGS_FOLDER ${AIRFLOW_USER_HOME}/logs
ENV AIRFLOW__CORE__EXECUTOR SequentialExecutor
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

# Airflow admin user
ENV AIRFLOW_ADMIN_USER admin
ENV AIRFLOW_ADMIN_EMAIL kientd.aits@vietnamairlines.com
ENV AIRFLOW_ADMIN_PASS Admin@123

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Define PYTHONPATH Augment the default search path for module files
ENV PYTHONPATH ${AIRFLOW_USER_HOME}

# Define Google Cloud Key
ENV GOOGLE_APPLICATION_CREDENTIALS ${AIRFLOW_USER_HOME}/resources/keys/dwh-demo-afab0e86006e.json

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,password,celery,postgres,hive,jdbc,mysql,mssql,oracle,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.2' \
    && pip install requests \
    && pip install avro-python3 \
    && pip install google-cloud-bigquery \
    && pip install google-cloud-storage \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY script/entrypoint.sh /entrypoint.sh
COPY script/airflow_admin_script.py /airflow_admin_script.py
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
