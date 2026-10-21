#!/usr/bin/env bash
# Crée .env à partir de .env.example en générant les secrets Airflow.
# Ne remplace jamais un .env existant.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
    echo ".env existe déjà : rien à faire"
    exit 0
fi

secret() { python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }

# Sous Linux, les fichiers créés par Airflow dans logs/ doivent appartenir à l'utilisateur courant.
uid=50000
if [ "$(uname -s)" = "Linux" ]; then uid=$(id -u); fi

sed -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=${uid}|" \
    -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$(secret)|" \
    -e "s|^AIRFLOW_API_SECRET_KEY=.*|AIRFLOW_API_SECRET_KEY=$(secret)|" \
    -e "s|^AIRFLOW_JWT_SECRET=.*|AIRFLOW_JWT_SECRET=$(secret)|" \
    .env.example > .env

echo ".env créé avec des secrets Airflow générés"
