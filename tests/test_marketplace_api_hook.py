"""Tests du MarketplaceAPIHook.

Un vrai serveur HTTP local (thread) rejoue des réponses scriptées : on teste
ainsi le retry d'urllib3 réellement, ce qu'un mock de Session.get court-circuiterait.
"""

import json
import threading
import time
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

import pytest
import requests
from airflow.sdk import Connection

from hooks.marketplace_api_hook import MarketplaceAPIHook

TOKEN = "test-token"


class FakeAPI:
    """Serveur HTTP local : chaque requête consomme la prochaine réponse scriptée."""

    def __init__(self):
        self.responses = []  # (status, body, delay_s) ; la dernière est répétée
        self.requests = []   # (path, headers)
        api = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                api.requests.append((self.path, dict(self.headers)))
                status, body, delay = api.responses[0] if len(api.responses) == 1 else api.responses.pop(0)
                time.sleep(delay)
                payload = json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # le client a abandonné (timeout)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def script(self, *responses):
        self.responses = [r if len(r) == 3 else (*r, 0) for r in responses]


@pytest.fixture
def api():
    fake = FakeAPI()
    yield fake
    fake.server.shutdown()
    fake.server.server_close()


def make_hook(host, password=TOKEN, port=None, **kwargs):
    conn = Connection(conn_id="marketplace_api", conn_type="http", host=host, password=password, port=port)
    kwargs.setdefault("backoff_factor", 0)
    hook = MarketplaceAPIHook(**kwargs)
    patcher = mock.patch.object(MarketplaceAPIHook, "get_connection", return_value=conn)
    patcher.start()
    return hook, patcher


@pytest.fixture
def hook_for():
    patchers = []

    def _factory(host, **kwargs):
        hook, patcher = make_hook(host, **kwargs)
        patchers.append(patcher)
        return hook

    yield _factory
    for patcher in patchers:
        patcher.stop()


ORDER = {"order_id": "ORD-20260407-00001", "total_amount": 60.79}


def test_get_orders_sends_bearer_token_from_connection(api, hook_for):
    api.script((200, [ORDER]))
    hook = hook_for(api.url)

    assert hook.get_orders("2026-04-07") == [ORDER]

    path, headers = api.requests[0]
    assert path == "/orders?date=2026-04-07"
    assert headers["Authorization"] == f"Bearer {TOKEN}"


def test_get_orders_accepts_date_objects(api, hook_for):
    api.script((200, []))
    hook = hook_for(api.url)

    hook.get_orders(date(2026, 4, 7))

    assert api.requests[0][0] == "/orders?date=2026-04-07"


def test_get_orders_rejects_invalid_date_without_calling_api(api, hook_for):
    hook = hook_for(api.url)

    with pytest.raises(ValueError, match="Date invalide"):
        hook.get_orders("07/04/2026")
    assert api.requests == []


def test_sellers_and_customers_forward_limit(api, hook_for):
    api.script((200, []))
    hook = hook_for(api.url)

    hook.get_sellers(limit=5)
    hook.get_customers()
    hook.get_products()

    assert [path for path, _ in api.requests] == ["/sellers?limit=5", "/customers", "/products"]


def test_retries_on_5xx_then_succeeds(api, hook_for):
    api.script((503, {"error": "down"}), (502, {"error": "down"}), (200, [ORDER]))
    hook = hook_for(api.url, max_retries=3)

    assert hook.get_orders("2026-04-07") == [ORDER]
    assert len(api.requests) == 3


def test_raises_http_error_when_retries_exhausted(api, hook_for):
    api.script((503, {"error": "down"}))
    hook = hook_for(api.url, max_retries=2)

    with pytest.raises(requests.HTTPError) as exc:
        hook.get_orders("2026-04-07")
    assert exc.value.response.status_code == 503
    assert len(api.requests) == 3  # 1 essai + 2 retries


def test_does_not_retry_on_401(api, hook_for):
    api.script((401, {"error": "unauthorized"}))
    hook = hook_for(api.url, max_retries=3)

    with pytest.raises(requests.HTTPError) as exc:
        hook.get_orders("2026-04-07")
    assert exc.value.response.status_code == 401
    assert len(api.requests) == 1


def test_retries_on_read_timeout(api, hook_for):
    api.script((200, [], 1.0), (200, [ORDER]))
    hook = hook_for(api.url, timeout=0.3, max_retries=2)

    assert hook.get_orders("2026-04-07") == [ORDER]
    assert len(api.requests) == 2


def test_rejects_non_list_payload(api, hook_for):
    api.script((200, {"orders": []}))
    hook = hook_for(api.url)

    with pytest.raises(ValueError, match="liste attendue"):
        hook.get_orders("2026-04-07")


def test_missing_token_in_connection_raises(hook_for):
    hook = hook_for("http://api-marketplace:5000", password=None)

    with pytest.raises(ValueError, match="token Bearer"):
        hook.get_conn()


def test_host_without_scheme_uses_connection_port(hook_for):
    hook = hook_for("api-marketplace", port=5000)

    hook.get_conn()

    assert hook.base_url == "http://api-marketplace:5000"


def test_live_api_is_deterministic():
    """Contre l'API du docker-compose (via la Connection réelle) ; ignoré si elle est injoignable."""
    hook = MarketplaceAPIHook(max_retries=0)
    try:
        hook.health()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"API Marketplace injoignable : {exc}")

    first = hook.get_orders("2026-04-07")
    second = hook.get_orders("2026-04-07")

    assert first and first == second
    assert {"order_id", "seller_id", "product_id", "total_amount", "status"} <= first[0].keys()
