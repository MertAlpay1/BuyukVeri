FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==2.22.0

EXPOSE 5000

CMD ["mlflow", "server", \
     "--backend-store-uri", "/home/mlflow/mlruns", \
     "--default-artifact-root", "/home/mlflow/mlruns", \
     "--host", "0.0.0.0", \
     "--port", "5000"]
