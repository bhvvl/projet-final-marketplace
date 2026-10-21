"""API Marketplace simulée (backoffice Maelys).

Données déterministes : le catalogue (vendeurs, produits, clients) est généré
à partir d'une graine fixe, les commandes à partir de seed = md5(date).
Une même date renvoie donc toujours exactement les mêmes commandes.

Défauts volontaires pour exercer la qualité de données et la détection
d'anomalies : quelques customer_id nuls, des prix suspects, des journées
en chute de ventes et des vendeurs qui arrêtent de vendre.
"""

import hashlib
import os
import random
from collections import deque
from datetime import date, datetime, timedelta
from functools import lru_cache

from flask import Flask, jsonify, request

API_VERSION = "1.0.0"
API_TOKEN = os.environ.get("API_TOKEN", "formation-token-2026")
ORDERS_PER_DAY = int(os.environ.get("ORDERS_PER_DAY", "1000"))
# Probabilité de répondre 503 (pour tester le retry du Hook). 0 par défaut.
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0"))

N_SELLERS = 150
N_PRODUCTS = 3000
N_CUSTOMERS = 8000
CATALOG_SEED = "maelys-catalog-v1"

CATEGORIES = {
    "Mode": (15, 120),
    "Maison & Déco": (10, 250),
    "Beauté": (5, 60),
    "High-Tech": (20, 600),
    "Sport": (10, 200),
    "Jouets": (8, 80),
    "Épicerie fine": (4, 45),
    "Bijoux": (15, 300),
    "Livres": (5, 35),
    "Jardin": (10, 180),
}
COUNTRIES = ["FR"] * 14 + ["BE", "ES", "IT", "DE", "CH"]
CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Lille", "Nantes",
          "Strasbourg", "Montpellier", "Rennes", "Nice", "Grenoble", "Dijon", "Brest"]
SELLER_TYPES = ["artisan", "dnvb", "revendeur"]
NAME_PREFIX = ["Atelier", "Maison", "Studio", "Comptoir", "Les Ateliers", "Boutique", "Fabrique"]
NAME_ROOT = ["Lumen", "Céleste", "Orme", "Sable", "Brume", "Garance", "Opale", "Cèdre",
             "Azur", "Velours", "Ambre", "Iris", "Silex", "Nacre", "Ocre", "Lin"]
PRODUCT_ADJ = ["Classique", "Premium", "Bio", "Vintage", "Édition limitée", "Essentiel", "Artisanal"]
PAYMENT_METHODS = ["card"] * 7 + ["paypal", "paypal", "apple_pay"]

app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False


def _rng(key: str) -> random.Random:
    return random.Random(int(hashlib.md5(key.encode()).hexdigest(), 16))


@lru_cache(maxsize=1)
def catalog():
    rng = _rng(CATALOG_SEED)

    sellers = []
    for i in range(1, N_SELLERS + 1):
        joined = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1800))
        sellers.append({
            "seller_id": f"S{i:04d}",
            "name": f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_ROOT)} {i}",
            "seller_type": rng.choice(SELLER_TYPES),
            "country": rng.choice(COUNTRIES),
            "city": rng.choice(CITIES),
            "joined_date": joined.isoformat(),
        })
    # ~6 % des vendeurs arrêtent de vendre (date de churn, interne).
    churn = {s["seller_id"]: date(2026, 1, 1) + timedelta(days=rng.randint(0, 300))
             for s in rng.sample(sellers, k=N_SELLERS * 6 // 100)}
    # Poids de vente : quelques gros vendeurs, beaucoup de petits.
    seller_weight = {s["seller_id"]: rng.paretovariate(1.3) for s in sellers}

    products = []
    for i in range(1, N_PRODUCTS + 1):
        category = rng.choice(list(CATEGORIES))
        lo, hi = CATEGORIES[category]
        seller = rng.choice(sellers)
        products.append({
            "product_id": f"P{i:05d}",
            "name": f"{category.split()[0]} {rng.choice(PRODUCT_ADJ)} #{i}",
            "category": category,
            "seller_id": seller["seller_id"],
            "base_price": round(rng.uniform(lo, hi), 2),
            "created_at": (date.fromisoformat(seller["joined_date"])
                           + timedelta(days=rng.randint(0, 200))).isoformat(),
        })

    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        customers.append({
            "customer_id": f"C{i:06d}",
            "email": f"client{i:06d}@example.com",
            "city": rng.choice(CITIES),
            "country": rng.choice(COUNTRIES),
            "signup_date": (date(2022, 1, 1) + timedelta(days=rng.randint(0, 1400))).isoformat(),
        })

    return {
        "sellers": sellers,
        "products": products,
        "customers": customers,
        "churn": churn,
        "product_weight": [seller_weight[p["seller_id"]] for p in products],
    }


def generate_orders(day: date):
    cat = catalog()
    rng = _rng(day.isoformat())

    volume = ORDERS_PER_DAY * rng.uniform(0.85, 1.15)
    if day.weekday() >= 5:
        volume *= 1.25
    if rng.random() < 0.07:  # journée "incident" : chute de ventes
        volume *= rng.uniform(0.35, 0.6)
    n_orders = int(volume)

    products = cat["products"]
    weights = [0 if cat["churn"].get(p["seller_id"], date.max) <= day else w
               for p, w in zip(products, cat["product_weight"])]
    picked = rng.choices(products, weights=weights, k=n_orders)
    customers = cat["customers"]

    orders = []
    for i, product in enumerate(picked, start=1):
        quantity = rng.choices([1, 2, 3, 4], weights=[70, 20, 7, 3])[0]
        unit_price = round(product["base_price"] * rng.uniform(0.9, 1.1), 2)
        roll = rng.random()
        if roll < 0.002:
            unit_price = round(product["base_price"] * rng.choice([10, 25]), 2)
        elif roll < 0.004:
            unit_price = 0.01
        status = rng.choices(["completed", "cancelled", "refunded"], weights=[93, 5, 2])[0]
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        customer_id = rng.choice(customers)["customer_id"]
        if rng.random() < 0.002:
            customer_id = None
        orders.append({
            "order_id": f"ORD-{day:%Y%m%d}-{i:05d}",
            "order_ts": ts.isoformat(),
            "customer_id": customer_id,
            "seller_id": product["seller_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(unit_price * quantity, 2),
            "currency": "EUR",
            "status": status,
            "payment_method": rng.choice(PAYMENT_METHODS),
        })
    return orders


def _limit(items):
    limit = request.args.get("limit", type=int)
    return items[:limit] if limit and limit > 0 else items


@app.before_request
def check_auth_and_chaos():
    if request.path == "/health":
        return None
    if request.headers.get("Authorization") != f"Bearer {API_TOKEN}":
        return jsonify(error="unauthorized", message="Missing or invalid Bearer token"), 401
    if FAILURE_RATE and random.random() < FAILURE_RATE:
        return jsonify(error="service_unavailable", message="Simulated failure"), 503
    return None


ALERTS = deque(maxlen=200)


@app.post("/webhook")
def webhook():
    """Récepteur d'alertes simulé (remplaçable par Slack/Teams en changeant la Connection Airflow)."""
    alert = {"received_at": datetime.now().isoformat(timespec="seconds"), "payload": request.get_json(silent=True)}
    ALERTS.append(alert)
    app.logger.warning("ALERTE reçue : %s", alert["payload"])
    return jsonify(status="accepted"), 202


@app.get("/webhook/alerts")
def webhook_alerts():
    return jsonify(list(ALERTS))


@app.get("/health")
def health():
    return jsonify(status="ok", version=API_VERSION)


@app.get("/orders")
def orders():
    raw = request.args.get("date")
    if not raw:
        return jsonify(error="bad_request", message="Query parameter 'date' (YYYY-MM-DD) is required"), 400
    try:
        day = date.fromisoformat(raw)
    except ValueError:
        return jsonify(error="bad_request", message="Invalid date, expected YYYY-MM-DD"), 400
    if day > date.today():
        return jsonify([])
    return jsonify(generate_orders(day))


@app.get("/sellers")
def sellers():
    return jsonify(_limit(catalog()["sellers"]))


@app.get("/products")
def products():
    return jsonify(catalog()["products"])


@app.get("/customers")
def customers():
    return jsonify(_limit(catalog()["customers"]))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
