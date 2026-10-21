"""Hook Airflow pour l'API backoffice de Maelys Marketplace."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import requests
from airflow.sdk import BaseHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

RETRY_STATUSES = (429, 500, 502, 503, 504)


class MarketplaceAPIHook(BaseHook):
    """Client de l'API Marketplace, authentifié par token Bearer.

    La Connection Airflow fournit l'URL de l'API dans ``host`` (avec ou sans
    schéma, ``port`` optionnel) et le token Bearer dans ``password``.

    Les erreurs transitoires (timeouts, connexion refusée, 429/5xx) sont
    rejouées avec un backoff exponentiel ; les erreurs client (401, 400…)
    lèvent immédiatement une ``requests.HTTPError``.

    :param marketplace_api_conn_id: identifiant de la Connection Airflow.
    :param timeout: timeout ``(connexion, lecture)`` en secondes.
    :param max_retries: nombre de nouvelles tentatives après le premier essai.
    :param backoff_factor: base du backoff exponentiel entre deux tentatives.
    """

    conn_name_attr = "marketplace_api_conn_id"
    default_conn_name = "marketplace_api"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(
        self,
        marketplace_api_conn_id: str = default_conn_name,
        timeout: float | tuple[float, float] = (5, 30),
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ) -> None:
        super().__init__()
        self.marketplace_api_conn_id = marketplace_api_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.base_url: str | None = None
        self._session: requests.Session | None = None

    def get_conn(self) -> requests.Session:
        """Construit (une seule fois) une session HTTP authentifiée avec retry."""
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.marketplace_api_conn_id)
        if not conn.host:
            raise ValueError(f"Connection '{self.marketplace_api_conn_id}' : 'host' (URL de l'API) manquant")
        if not conn.password:
            raise ValueError(f"Connection '{self.marketplace_api_conn_id}' : 'password' (token Bearer) manquant")

        base_url = conn.host.rstrip("/")
        if "://" not in base_url:
            base_url = f"http://{base_url}"
        if conn.port and base_url.count(":") < 2:
            base_url = f"{base_url}:{conn.port}"
        self.base_url = base_url

        retry = Retry(
            total=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=RETRY_STATUSES,
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,  # on laisse raise_for_status() produire une HTTPError lisible
        )
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Authorization": f"Bearer {conn.password}", "Accept": "application/json"})

        self._session = session
        return session

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.log.info("GET %s params=%s", url, params)
        response = session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _get_list(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        payload = self._get(endpoint, params)
        if not isinstance(payload, list):
            raise ValueError(f"Réponse inattendue de /{endpoint} : liste attendue, reçu {type(payload).__name__}")
        self.log.info("/%s : %d enregistrements", endpoint, len(payload))
        return payload

    @staticmethod
    def _limit_params(limit: int | None) -> dict[str, int] | None:
        return {"limit": limit} if limit else None

    def health(self) -> dict[str, Any]:
        return self._get("health")

    def get_orders(self, day: str | date | datetime) -> list[dict[str, Any]]:
        """Commandes d'une journée (``YYYY-MM-DD``, ``date`` ou ``datetime``)."""
        if isinstance(day, datetime):
            day = day.date()
        if isinstance(day, date):
            day = day.isoformat()
        try:
            date.fromisoformat(day)
        except (TypeError, ValueError):
            raise ValueError(f"Date invalide : {day!r} (format attendu YYYY-MM-DD)") from None
        return self._get_list("orders", {"date": day})

    def get_sellers(self, limit: int | None = None) -> list[dict[str, Any]]:
        return self._get_list("sellers", self._limit_params(limit))

    def get_products(self) -> list[dict[str, Any]]:
        return self._get_list("products")

    def get_customers(self, limit: int | None = None) -> list[dict[str, Any]]:
        return self._get_list("customers", self._limit_params(limit))

    def test_connection(self) -> tuple[bool, str]:
        """Utilisé par le bouton "Test" de l'UI : vérifie l'URL ET le token."""
        try:
            self.get_sellers(limit=1)
        except Exception as exc:  # noqa: BLE001 — on remonte le message à l'UI
            return False, str(exc)
        return True, "Connexion à l'API Marketplace OK"
