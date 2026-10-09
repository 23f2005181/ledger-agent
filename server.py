import os
import re
import io
import csv
import json
import calendar
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from flask import Flask, request, jsonify

app = Flask(__name__)
log = app.logger

ROOT_URL = os.environ.get("LEDGER_URL", "").strip()
TIMEOUT = 10
IST = ZoneInfo("Asia/Kolkata")
UTC = timezone.utc
MIN_DT = datetime.min.replace(tzinfo=UTC)
ZERO = Decimal(0)

session = requests.Session()
session.headers.update({"User-Agent": "AcmeLedgerAgent/2.0"})
session.mount("https://", HTTPAdapter(max_retries=2, pool_connections=10, pool_maxsize=10))


# ----------------------------------------------------------------- fetching
def fetch(url):
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    ctype = response.headers.get("Content-Type", "").lower()
    if "json" in ctype:
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
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) > 1:
        try:
            parsed = [json.loads(ln) for ln in lines]
            if all(isinstance(i, (dict, list)) for i in parsed):
                return parsed
        except (ValueError, TypeError):
            pass
    if "," in text and "\n" in text:
        try:
            return list(csv.DictReader(io.StringIO(text)))
        except (csv.Error, ValueError):
            pass
    return text


def find_rows(data):
    if isinstance(data, list):
        if all(isinstance(i, dict) for i in data):
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
        if isinstance(value, list) and value and all(isinstance(i, dict) for i in value):
            return value
    return []


PAGE_KEYS = ("pages", "total_pages", "totalPages", "page_count", "num_pages")


def total_pages_of(payload):
    if not isinstance(payload, dict):
        return 0
    for src in (payload, payload.get("meta"), payload.get("pagination")):
        if isinstance(src, dict):
            for k in PAGE_KEYS:
                try:
                    v = int(src.get(k))
                    if v:
                        return v
                except (TypeError, ValueError):
                    continue
    return 0


def page_url(first_url, n):
    parts = urlparse(first_url)
    params = parse_qs(parts.query)
    params["page"] = [str(n)]
    return urlunparse(parts._replace(query=urlencode(params, doseq=True)))


def fetch_all_pages(first_url):
    first_payload = fetch(first_url)
    rows = list(find_rows(first_payload))
    if not isinstance(first_payload, dict):
        return rows

    total = total_pages_of(first_payload)
    if total > 1:
        def one(n):
            return find_rows(fetch(page_url(first_url, n)))
        with ThreadPoolExecutor(max_workers=8) as pool:
            for part in pool.map(one, range(2, total + 1)):
                rows.extend(part)
        return rows

    seen = {first_url}
    payload = first_payload
    page_no = 1
    while isinstance(payload, dict):
        nxt = payload.get("next") or payload.get("next_page") or payload.get("next_url")
        if not nxt:
            break
        if isinstance(nxt, int) or (isinstance(nxt, str) and nxt.isdigit()):
            page_no = int(nxt)
            url = page_url(first_url, page_no)
        else:
            url = urljoin(first_url, str(nxt))
        if url in seen:
            break
        seen.add(url)
        payload = fetch(url)
        part = find_rows(payload)
        if not part:
            break
        rows.extend(part)
    return rows


# ------------------------------------------------------------------ helpers
def key_norm(value):
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def get_value(row, aliases, default=None):
    if not isinstance(row, dict):
        return default
    normalized = {key_norm(k): v for k, v in row.items()}
    for alias in aliases:
        k = key_norm(alias)
        if k in normalized and normalized[k] not in (None, ""):
            return normalized[k]
    return default


def num(value):
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = re.sub(r"[^\d.\-]", "", str(value).replace(",", ""))
    if text in ("", "-", "."):
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


FALLBACK_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y",
                    "%m/%d/%Y", "%d %b %Y", "%b %d, %Y")


def parse_dt(value):
    """Return an aware datetime in Asia/Kolkata. Naive values are taken as IST."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e11:
            v /= 1000.0
        try:
            return datetime.fromtimestamp(v, tz=UTC).astimezone(IST)
        except (ValueError, OSError, OverflowError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d{9,13}(\.\d+)?", text):
        return parse_dt(float(text))
    iso = re.sub(r"[Zz]$", "+00:00", text)
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        dt = None
        for fmt in FALLBACK_FORMATS:
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


ID_KEYS = ["order_id", "orderId", "id", "order_number"]
UPDATED_KEYS = ["updated_at", "updatedAt", "last_updated", "modified_at"]
DATE_KEYS = ["order_date", "date", "created_at", "createdAt", "placed_at", "ordered_at", "timestamp"]
USD_KEYS = ["total_usd", "amount_usd", "revenue_usd"]
AMOUNT_KEYS = ["order_total", "total_amount", "total", "amount", "line_total", "net_amount", "price"]
REFUND_USD_KEYS = ["refund_usd", "refund_amount_usd"]
REFUND_KEYS = ["refunded_amount", "refund_amount", "total_refunds", "refund", "amount_refunded"]
REFUND_STATUSES = {"refunded", "partiallyrefunded", "partialrefund", "partialrefunded"}


def latest_orders(rows):
    latest = {}
    without_id = []
    for row in rows:
        oid = get_value(row, ID_KEYS)
        if oid is None:
            without_id.append(row)
            continue
        upd = parse_dt(get_value(row, UPDATED_KEYS)) or MIN_DT
        k = str(oid).strip()
        prev = latest.get(k)
        if prev is None or upd >= prev[0]:
            latest[k] = (upd, row)
    return [item[1] for item in latest.values()] + without_id


def row_date(row):
    dt = parse_dt(get_value(row, DATE_KEYS))
    return dt.date() if dt else None


def row_status(row):
    return key_norm(get_value(row, ["status", "order_status"], ""))


def row_region(row):
    return str(get_value(row, ["region", "sales_region", "territory"], "")).strip()


def row_product(row):
    return str(get_value(row, ["product_name", "product", "item_name", "item", "sku_name"], "")).strip()


def row_customer(row):
    return str(get_value(row, ["customer_name", "customer", "buyer_name", "client_name"], "")).strip()


def row_units(row):
    q = num(get_value(row, ["qty", "quantity", "units"]))
    return q if q is not None else Decimal(1)


def rate_table(rates_data):
    if not isinstance(rates_data, dict):
        return {}
    table = rates_data
    for key in ("usd_per_unit", "rates", "usd_rates"):
        if isinstance(rates_data.get(key), dict):
            table = rates_data[key]
            break
    out = {}
    for k, v in table.items():
        n = num(v)
        if n is not None:
            out[str(k).upper()] = n
    return out


def rate_for(row, rates):
    cur = str(get_value(row, ["currency", "currency_code"], "USD")).strip().upper()
    if cur in ("USD", "US$", ""):
        return Decimal(1)
    r = rates.get(cur)
    if r is None:
        raise ValueError("No USD rate for currency %s" % cur)
    return r


def amount_usd(row, rates):
    direct = num(get_value(row, USD_KEYS))
    if direct is not None:
        return direct
    amount = num(get_value(row, AMOUNT_KEYS))
    if amount is None:
        cents = num(get_value(row, ["amount_cents", "total_cents"]))
        if cents is not None:
            amount = cents / 100
    if amount is None:
        unit = num(get_value(row, ["unit_price", "price_each"]))
        if unit is not None:
            amount = unit * row_units(row)
    if amount is None:
        return ZERO
    return amount * rate_for(row, rates)


def refund_usd(row, rates):
    direct = num(get_value(row, REFUND_USD_KEYS))
    if direct is not None and direct > 0:
        return direct
    val = num(get_value(row, REFUND_KEYS))
    if val is not None and val > 0:
        return val * rate_for(row, rates)
    if row_status(row) in REFUND_STATUSES:
        return amount_usd(row, rates)
    return ZERO


def is_refund_row(row, rates):
    return row_status(row) in REFUND_STATUSES or refund_usd(row, rates) > 0


def money(x):
    return float(Decimal(x).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


# ------------------------------------------------------------------ loading
def build_index(rows):
    def uniq(fn):
        vals = {fn(r).lower() for r in rows if fn(r)}
        return sorted((v for v in vals if len(v) >= 2), key=len, reverse=True)
    return uniq(row_region), uniq(row_product), uniq(row_customer)


def _load_uncached():
    if not ROOT_URL:
        raise RuntimeError("LEDGER_URL environment variable is not configured")
    root = fetch(ROOT_URL)
    if not isinstance(root, dict):
        raise RuntimeError("Ledger root endpoint did not return JSON")
    links = root.get("links") if isinstance(root.get("links"), dict) else {}

    candidates = {}
    if links.get("orders"):
        try:
            candidates["orders"] = latest_orders(fetch_all_pages(links["orders"]))
        except Exception:
            log.exception("orders fetch failed")
    if links.get("export"):
        try:
            candidates["export"] = latest_orders(find_rows(fetch(links["export"])))
        except Exception:
            log.exception("export fetch failed")
    candidates = {k: v for k, v in candidates.items() if v}
    if not candidates:
        raise RuntimeError("No order rows found")
    best = max(candidates, key=lambda k: len(candidates[k]))
    rows = candidates[best]
    log.info("ledger sources: %s -> using %s", {k: len(v) for k, v in candidates.items()}, best)

    rates = {}
    if links.get("rates"):
        try:
            rates = rate_table(fetch(links["rates"]))
        except Exception:
            log.exception("rates fetch failed")

    regions, products, customers = build_index(rows)
    return {"rows": rows, "rates": rates, "regions": regions,
            "products": products, "customers": customers, "source": best}


_CACHE = None
_LOCK = threading.Lock()


def load_ledger():
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    with _LOCK:
        if _CACHE is None:
            _CACHE = _load_uncached()
        return _CACHE


try:
    load_ledger()
    log.info("Ledger cache warmed.")
except Exception:
    log.exception("Ledger warm-up failed; will retry on first question.")


# ------------------------------------------------------------ question parse
MONTHS = {}
for _i in range(1, 13):
    MONTHS[calendar.month_name[_i].lower()] = _i
    MONTHS[calendar.month_abbr[_i].lower()] = _i
MONTHS["sept"] = 9


def pull_entity(values, orig, low):
    for v in values:
        m = re.search(r"(?<![a-z0-9])" + re.escape(v) + r"(?![a-z0-9])", low)
        if m:
            return (v, orig[:m.start()] + " " + orig[m.end():], low[:m.start()] + " " + low[m.end():])
    return None, orig, low


def find_months(text):
    found = []
    for m in re.finditer(r"\b([A-Za-z]{3,9})\b", text):
        w = m.group(1).lower()
        if w not in MONTHS:
            continue
        if w == "may":
            tail = text[m.end():m.end() + 14]
            ok = (m.group(1) == "May"
                  and not re.match(r"\s+(i|you|we|they|he|she|it|be|have|not)\b", tail, re.I)) \
                or re.match(r"\s*,?\s*20\d\d", tail)
            if not ok:
                continue
        found.append(MONTHS[w])
    return found


def parse_period(text):
    years = {int(y) for y in re.findall(r"\b(20\d{2})\b", text)}
    months = set()
    found = find_months(text)
    if found:
        if (len(found) == 2 and found[0] <= found[1]
                and re.search(r"\b(between|from|through|thru|to|till|until)\b|[\u2013\u2014]", text, re.I)):
            months = set(range(found[0], found[1] + 1))
        else:
            months = set(found)
    else:
        ords = {"first": 1, "1st": 1, "second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4}
        qn = None
        m = re.search(r"\bq([1-4])\b", text, re.I)
        if m:
            qn = int(m.group(1))
        else:
            m = re.search(r"\b(first|second|third|fourth|1st|2nd|3rd|4th)\s+quarter\b", text, re.I)
            if m:
                qn = ords[m.group(1).lower()]
        if qn:
            months = set(range(3 * (qn - 1) + 1, 3 * qn + 1))
        else:
            m = re.search(r"\bh([12])\b|\b(first|second)\s+half\b", text, re.I)
            if m:
                h = int(m.group(1)) if m.group(1) else (1 if m.group(2).lower() == "first" else 2)
                months = set(range(1, 7)) if h == 1 else set(range(7, 13))
    return months, years


def in_scope(row, region, product, customer, months, years):
    if region and row_region(row).lower() != region:
        return False
    if product and row_product(row).lower() != product:
        return False
    if customer and row_customer(row).lower() != customer:
        return False
    if months or years:
        d = row_date(row)
        if d is None:
            return False
        if months and d.month not in months:
            return False
        if years and d.year not in years:
            return False
    return True


def top_items(rows, namefn, rates, by_units, n):
    totals = {}
    for r in rows:
        name = namefn(r)
        if not name:
            continue
        val = row_units(r) if by_units else amount_usd(r, rates)
        totals[name] = totals.get(name, ZERO) + val
    ranked = sorted(totals.items(), key=lambda kv: (-kv[1], kv[0]))
    if not ranked:
        return None
    if n and n > 1:
        return [k for k, _ in ranked[:n]]
    return ranked[0][0]


STATUS_HINTS = ("cancel", "pending", "fail", "ship", "deliver", "return")


def answer_question(question):
    L = load_ledger()
    rows, rates = L["rows"], L["rates"]
    qf = question.strip().lower()

    orig, low = question.strip(), qf
    product, orig, low = pull_entity(L["products"], orig, low)
    customer, orig, low = pull_entity(L["customers"], orig, low)
    region, orig, low = pull_entity(L["regions"], orig, low)
    months, years = parse_period(orig)

    selected = [r for r in rows if in_scope(r, region, product, customer, months, years)]
    paid = [r for r in selected if row_status(r) == "paid"]

    # --- refunds
    if re.search(r"refund|money returned", qf):
        refunded = [r for r in selected if is_refund_row(r, rates)]
        asks_money = re.search(r"\b(usd|dollars?|how much|amount)\b|\$", qf)
        asks_count = re.search(r"\b(how many|number of|count)\b", qf)
        if asks_count and not asks_money:
            return len(refunded)
        return money(sum((refund_usd(r, rates) for r in refunded), ZERO))

    # --- average order value
    if (re.search(r"\b(average|avg|mean)\b", qf) or "per order" in qf
            or ("worth" in qf and "order" in qf)):
        if not paid:
            return 0
        return money(sum((amount_usd(r, rates) for r in paid), ZERO) / len(paid))

    # --- top product / customer
    topword = re.search(r"\b(top|best|highest|most|biggest|leading|best-selling|bestselling)\b|which (product|customer)", qf)
    nmatch = re.search(r"\btop\s+(\d+)\b", qf)
    n = int(nmatch.group(1)) if nmatch else None
    by_units = bool(re.search(r"\b(units?|quantity|qty|pieces)\b", qf))
    if topword and re.search(r"\bproducts?\b", qf):
        return top_items(paid, row_product, rates, by_units, n)
    if topword and re.search(r"\bcustomers?\b", qf):
        return top_items(paid, row_customer, rates, by_units, n)

    # --- order counts
    if re.search(r"how many orders|number of orders|count of orders|order count|total orders", qf):
        if re.search(r"\bunpaid\b", qf):
            return len([r for r in selected if row_status(r) != "paid"])
        for hint in STATUS_HINTS:
            if hint in qf:
                return len([r for r in selected if hint in row_status(r)])
        if re.search(r"\bpaid\b", qf):
            return len(paid)
        return len(selected)

    # --- units sold
    if by_units and re.search(r"\b(how many|number of|total)\b", qf):
        return int(sum((row_units(r) for r in paid), ZERO)) if all(
            row_units(r) == int(row_units(r)) for r in paid) else float(
            sum((row_units(r) for r in paid), ZERO))

    # --- distinct customers
    if (re.search(r"\b(how many|number of|count of|count)\b", qf)
            and re.search(r"\b(customers|distinct customer|unique customer)\b", qf)) \
            or re.search(r"customer count|distinct customers|unique customers", qf):
        pool = paid if re.search(r"\bpaid\b", qf) else selected
        return len({row_customer(r).lower() for r in pool if row_customer(r)})

    # --- default: revenue in USD from paid orders
    return money(sum((amount_usd(r, rates) for r in paid), ZERO))


# ------------------------------------------------------------------- routes
@app.route("/", methods=["GET"])
def health():
    return jsonify({"service": "Acme Appliances Ledger Agent", "status": "ok"})


@app.route("/", methods=["POST"])
@app.route("/ask", methods=["POST"])
def ask():
    body = request.get_json(silent=True) or {}
    question = body.get("question")
    if not isinstance(question, str) or not question.strip():
        return jsonify({"error": "JSON field 'question' is required"}), 400
    try:
        return jsonify({"answer": answer_question(question)})
    except Exception:
        log.exception("Question processing failed")
        return jsonify({"error": "Could not process question"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
