# docker/spark.dockerfile
FROM apache/spark:4.0.1-python3

USER root

# Installer Python 3.12 depuis les sources Debian
RUN apt-get update && \
    apt-get install -y wget build-essential libssl-dev zlib1g-dev \
    libncurses5-dev libncursesw5-dev libreadline-dev libsqlite3-dev \
    libgdbm-dev libdb5.3-dev libbz2-dev libexpat1-dev liblzma-dev \
    tk-dev libffi-dev && \
    cd /tmp && \
    wget https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz && \
    tar xzf Python-3.12.0.tgz && \
    cd Python-3.12.0 && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j $(nproc) && \
    make altinstall && \
    cd / && \
    rm -rf /tmp/Python-3.12.0* && \
    apt-get remove -y wget build-essential && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Créer les liens symboliques
RUN ln -sf /usr/local/bin/python3.12 /usr/bin/python3 && \
    ln -sf /usr/local/bin/python3.12 /usr/bin/python && \
    ln -sf /usr/local/bin/pip3.12 /usr/bin/pip3 && \
    ln -sf /usr/local/bin/pip3.12 /usr/bin/pip

ENV PYSPARK_PYTHON=/usr/local/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

USER ${spark_uid}