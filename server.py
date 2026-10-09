
import os
import re
import csv
import io
import json
import calendar
from datetime import datetime, date
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

ROOT_URL = os.environ.get("LEDGER_URL", "").strip()
TIMEOUT = 10

session = requests.Session()
session.headers.update({"User-Agent": "AcmeLedgerAgent/1.0"})


def fetch(url):
    """Fetch JSON, JSON Lines, or CSV data."""
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "").lower()

    if "json" in content_type:
        try:
            return response.json()
        except ValueError:
            pass

    text = response.text.strip()
    if not text:
        return []

    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass

    # Some export endpoints return one JSON object per line.
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) > 1:
        try:
            parsed = [json.loads(line) for line in lines]
            if all(isinstance(item, (dict, list)) for item in parsed):
                return parsed
        except (ValueError, TypeError):
            pass

    if "," in text and "\\n" in text:
        try:
            return list(csv.DictReader(io.StringIO(text)))
        except (csv.Error, ValueError):
            pass

    return text


def find_rows(data):
    """Extract row-like dictionaries from common API response formats."""
    if isinstance(data, list):
        if all(isinstance(item, dict) for item in data):
            return data
        rows = []
        for item in data:
            rows.extend(find_rows(item))
        return rows

    if not isinstance(data, dict):
        return []

    for key in ("orders", "rows", "records", "results", "items", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return find_rows(value)

    for value in data.values():
        if isinstance(value, list) and value and all(
            isinstance(item, dict) for item in value
        ):
            return value

    return []


def fetch_all_pages(first_url):
    """Follow pagination links or increment a page parameter."""
    all_rows = []
    seen_urls = set()
    url = first_url

    for page_number in range(1, 501):
        if not url or url in seen_urls:
            break
        seen_urls.add(url)

        payload = fetch(url)
        rows = find_rows(payload)

        if not rows:
            break

        all_rows.extend(rows)

        if not isinstance(payload, dict):
            break

        next_url = payload.get("next")
        if not next_url:
            links = payload.get("links", {})
            if isinstance(links, dict):
                next_url = links.get("next")

        if next_url:
            url = requests.compat.urljoin(url, next_url)
            continue

        # If the response reports pagination, use it.
        pagination = payload.get("pagination", {})
        has_more = (
            isinstance(pagination, dict)
            and pagination.get("has_more") is True
        )
        if has_more:
            parts = urlparse(first_url)
            params = parse_qs(parts.query)
            params["page"] = [str(page_number + 1)]
            url = urlunparse(
                parts._replace(query=urlencode(params, doseq=True))
            )
            continue

        break

    return all_rows


def key_norm(value):
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def get_value(row, aliases, default=None):
    """Look up fields despite variations in capitalization or separators."""
    if not isinstance(row, dict):
        return default

    normalized = {key_norm(k): v for k, v in row.items()}
    for alias in aliases:
        key = key_norm(alias)
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return default


def parse_number(value):
    if isinstance(value, (int, float)):
        return float(value)

    if value is None:
        return None

    text = str(value).strip().replace(",", "")
    text = re.sub(r"[$₹£€]", "", text)

    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value):
    if not value:
        return None

    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value).date()
        except (ValueError, OSError, OverflowError):
            return None

    text = str(value).strip()
    for fmt in (
        "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%m/%d/%Y",
        "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(text[:26], fmt).date()
        except ValueError:
            pass

    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def latest_orders(rows):
    """Keep the most recently updated record for each order ID."""
    latest = {}
    without_id = []

    for row in rows:
        order_id = get_value(
            row, ["order_id", "orderId", "id", "order_number"]
        )
        if order_id is None:
            without_id.append(row)
            continue

        updated = str(get_value(
            row, ["updated_at", "updatedAt", "last_updated"], ""
        ))

        key = str(order_id)
        previous = latest.get(key)

        if previous is None or updated > previous[0]:
            latest[key] = (updated, row)

    return [item[1] for item in latest.values()] + without_id


def load_ledger():
    if not ROOT_URL:
        raise RuntimeError("LEDGER_URL environment variable is not configured")

    root = fetch(ROOT_URL)
    if not isinstance(root, dict):
        raise RuntimeError("Ledger root endpoint did not return JSON")

    links = root.get("links", {})
    if not isinstance(links, dict):
        links = {}

    orders = []
    export_data = None
    rates_data = None

    # Prefer the export endpoint if it contains the complete ledger.
    if links.get("export"):
        try:
            export_data = fetch(links["export"])
            orders = find_rows(export_data)
        except requests.RequestException:
            pass

    if not orders and links.get("orders"):
        orders = fetch_all_pages(links["orders"])

    if links.get("rates"):
        rates_data = fetch(links["rates"])

    if not orders:
        raise RuntimeError(
            "No order rows found. Inspect the orders/export response schema."
        )

    return latest_orders(orders), rates_data, export_data


def row_date(row):
    return parse_date(get_value(
        row, ["order_date", "date", "created_at", "createdAt", "timestamp"]
    ))


def row_status(row):
    return str(get_value(row, ["status", "order_status"], "")).lower()


def row_amount(row):
    return parse_number(get_value(
        row,
        [
            "total_usd", "amount_usd", "revenue_usd",
            "order_total", "total_amount", "total",
            "amount", "line_total", "net_amount", "price",
        ],
    ))



def amount_usd(row, rates_data):
    """Convert an order amount to USD using the ledger's usd_per_unit rates."""
    direct = parse_number(get_value(
        row, ["total_usd", "amount_usd", "revenue_usd"]
    ))
    if direct is not None:
        return direct

    amount = parse_number(get_value(
        row, ["order_total", "total_amount", "total", "amount",
              "line_total", "net_amount", "price"]
    ))

    if amount is None:
        unit_price = parse_number(get_value(row, ["unit_price"]))
        quantity = parse_number(get_value(row, ["qty", "quantity"]))
        if unit_price is not None:
            amount = unit_price * (quantity if quantity is not None else 1)

    if amount is None:
        return 0.0

    currency = str(get_value(row, ["currency"], "USD")).upper()
    rates = rates_data.get("usd_per_unit", {}) if isinstance(rates_data, dict) else {}
    rate = parse_number(rates.get(currency))

    if currency == "USD":
        rate = 1.0
    elif rate is None:
        # Do not silently apply a fabricated exchange rate.
        raise ValueError(f"No USD conversion rate for currency {currency}")

    return amount * rate


def row_refund(row):
    return parse_number(get_value(
        row,
        [
            "refund_usd", "refund_amount_usd",
            "refunded_amount", "refund_amount",
            "total_refunds", "refund", "amount_refunded",
        ],
    )) or 0.0


def row_region(row):
    return str(get_value(row, ["region", "sales_region", "territory"], ""))


def row_product(row):
    return str(get_value(
        row, ["product_name", "product", "item_name", "item", "sku_name"], ""
    ))


def row_customer(row):
    return str(get_value(
        row, ["customer_name", "customer", "buyer_name", "client_name"], ""
    ))


def extract_month_year(question):
    q = question.lower()
    months = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
    months.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})

    month = None
    for name, number in months.items():
        if re.search(r"\b" + re.escape(name) + r"\b", q):
            month = number
            break

    year_match = re.search(r"\b(20\d{2})\b", q)
    year = int(year_match.group(1)) if year_match else None
    return month, year


def filter_rows(rows, question):
    q = question.lower()
    month, year = extract_month_year(q)
    selected = []

    regions = sorted(
        {row_region(r) for r in rows if row_region(r)},
        key=len,
        reverse=True,
    )
    products = sorted(
        {row_product(r) for r in rows if row_product(r)},
        key=len,
        reverse=True,
    )
    customers = sorted(
        {row_customer(r) for r in rows if row_customer(r)},
        key=len,
        reverse=True,
    )

    region = next((v for v in regions if v.lower() in q), None)
    product = next((v for v in products if v.lower() in q), None)
    customer = next((v for v in customers if v.lower() in q), None)

    for row in rows:
        d = row_date(row)

        if month and (not d or d.month != month):
            continue
        if year and (not d or d.year != year):
            continue
        if region and row_region(row).lower() != region.lower():
            continue
        if product and row_product(row).lower() != product.lower():
            continue
        if customer and row_customer(row).lower() != customer.lower():
            continue

        selected.append(row)

    return selected


def answer_question(question):
    rows, rates, export_data = load_ledger()
    q = question.lower()
    selected = filter_rows(rows, question)

    if not selected:
        return 0

    # Only paid orders count as revenue.
    paid = [r for r in selected if row_status(r) == "paid"]

    # Average value of paid orders, including USD conversion when requested.
    average_question = (
        any(word in q for word in ("average", "mean", "per order", "order worth"))
        or ("worth" in q and "order" in q)
    )
    if average_question and ("order" in q or "orders" in q):
        if not paid:
            return 0
        if re.search(r"\\b(usd|us dollars?|dollars?)\\b", q):
            values = [amount_usd(r, rates) for r in paid]
        else:
            values = [row_amount(r) or 0 for r in paid]
        return round(sum(values) / len(values), 2)

    if any(word in q for word in ("refund", "refunded", "money returned")):
        return round(sum(row_refund(r) for r in selected), 2)

    if any(word in q for word in ("how many orders", "number of orders",
                                  "count of orders", "order count")):
        return len(selected)

    # Count distinct customers, with optional paid-only restriction.
    customer_count_phrases = (
        "how many customers",
        "how many distinct customers",
        "distinct customers",
        "number of customers",
        "unique customers",
        "customer count",
        "customers have bought",
        "customers bought",
        "customers purchased",
        "customers have purchased",
    )

    if any(phrase in q for phrase in customer_count_phrases):
        paid_only_requested = any(phrase in q for phrase in (
            "paid orders only",
            "paid only",
            "only paid",
            "paid orders",
        ))
        customer_rows = paid if paid_only_requested else selected
        return len({
            row_customer(r)
            for r in customer_rows
            if row_customer(r)
        })

    if any(word in q for word in ("top product", "top-selling product", "top selling product",
                                  "best-selling product", "best selling product",
                                  "bestselling product", "most popular product")):
        totals = {}
        for r in paid:
            name = row_product(r)
            if name:
                totals[name] = totals.get(name, 0) + amount_usd(r, rates)
        return max(totals, key=totals.get) if totals else None

    if any(word in q for word in ("top customer", "biggest customer",
                                  "highest spending customer")):
        totals = {}
        for r in paid:
            name = row_customer(r)
            if name:
                totals[name] = totals.get(name, 0) + amount_usd(r, rates)
        return max(totals, key=totals.get) if totals else None

    # Default: questions about revenue, sales or total sales.
    if any(word in q for word in ("revenue", "sales", "sold", "income", "total", "earn", "earned", "make", "made", "generate", "generated")):
        if re.search(r"\\b(usd|us dollars?|dollars?)\\b", q):
            return round(sum(amount_usd(r, rates) for r in paid), 2)
        return round(sum(row_amount(r) or 0 for r in paid), 2)

    return {
        "error": "Could not determine the requested metric",
        "hint": "Ask about revenue, refunds, orders, products, or customers.",
    }


@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "Acme Appliances Ledger Agent", "status": "ok"})


@app.route("/", methods=["POST"])
def ask():
    body = request.get_json(silent=True) or {}
    question = body.get("question")

    if not isinstance(question, str) or not question.strip():
        return jsonify({"error": "JSON field 'question' is required"}), 400

    try:
        answer = answer_question(question.strip())
        return jsonify({"answer": answer})
    except Exception:
        app.logger.exception("Question processing failed")
        return jsonify({"error": "Could not process question"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
