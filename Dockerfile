FROM apache/airflow:2.7.2

COPY .env /opt/airflow/.env

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    openpyxl \
    sqlalchemy \
    smbprotocol[cifs] \
    pymysql \
    XlsxWriter \
    "apache-airflow[postgres]"