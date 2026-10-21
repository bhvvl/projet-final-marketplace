FROM apache/airflow:3.1.8

# Providers amazon (S3Hook/MinIO), postgres, celery et fab sont déjà inclus dans l'image.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
