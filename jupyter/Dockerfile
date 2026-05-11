FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Notebook'larda kullanılan Python kütüphanelerini yükle
RUN pip install --no-cache-dir \
    deltalake==1.5.1 \
    pyarrow \
    matplotlib \
    seaborn \
    numpy \
    kafka-python

USER $NB_UID
