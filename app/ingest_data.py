"""
ingest_data.py — Load SAP O2C JSONL dataset into SQLite (data.db).

Expected layout under DATA_DIR:
  business_partners.jsonl                        → customers
  products.jsonl                                 → products
  product_texts.jsonl                            → product_descriptions
  sales_order_headers.jsonl                      → orders
  sales_order_items.jsonl                        → order_items
  outbound_delivery_headers.jsonl                → deliveries  (order_id resolved
  outbound_delivery_items.jsonl                  │  via this companion file)
  billing_document_headers.jsonl                 → billing     (delivery_id resolved
  billing_document_items.jsonl                   │  via this companion file)
  billing_document_cancellations.jsonl           → billing     (merged, is_cancelled=1)
  payments_accounts_receivable.jsonl             → payments
  journal_entry_items_accounts_receivable.jsonl  → journal_entries

Each source may also be a *directory* of part-*.jsonl files (as exported by SAP
extraction tools). The loader transparently handles both layouts.

ID normalisation
----------------
SAP pads many numeric IDs with leading zeros ("000010" → "10").
strip_zeros() is applied consistently to every FK before insert so that
cross-table joins always work.
"""

import glob
import json
import os
import sqlite3
from typing import Any, Generator

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH = "data.db"
DATA_DIR = "data/sap-o2c-data"

# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_zeros(value: Any) -> str | None:
    """
    Strip leading zeros from a SAP ID string.
    Returns None for empty/null values so FK columns stay NULL rather than ''.

    Examples
    --------
    >>> strip_zeros("000010")  →  "10"
    >>> strip_zeros("310000108")  →  "310000108"   # no change - no leading zeros
    >>> strip_zeros(None)  →  None
    >>> strip_zeros("")  →  None
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    stripped = s.lstrip("0")
    return stripped if stripped else "0"   # bare "0" → keep as "0", not ""


def to_float(value: Any) -> float | None:
    """Cast to float; return None for missing/empty values."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def iter_jsonl(source: str) -> Generator[dict, None, None]:
    """
    Yield parsed JSON objects from *source*.

    *source* can be:
      - a plain file path   ("data/products.jsonl")
      - a directory path    ("data/products/")   → reads all *.jsonl inside
      - a glob pattern      ("data/products/part-*.jsonl")

    Blank lines and lines that fail JSON parsing are silently skipped.
    """
    paths: list[str] = []

    if os.path.isdir(source):
        paths = sorted(glob.glob(os.path.join(source, "*.jsonl")))
    elif "*" in source or "?" in source:
        paths = sorted(glob.glob(source))
    elif os.path.isfile(source):
        paths = [source]

    if not paths:
        return   # source doesn't exist → caller handles the empty yield

    for path in paths:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def resolve_source(data_dir: str, *candidates: str) -> str:
    """
    Return the first existing path from *candidates* (checked as file and as
    directory).  Falls back to the last candidate so callers get a predictable
    path even when the file is absent (iter_jsonl handles the missing-file case).
    """
    for name in candidates:
        path = os.path.join(data_dir, name)
        if os.path.exists(path):
            return path
    return os.path.join(data_dir, candidates[-1])


# ── Lookup builders ───────────────────────────────────────────────────────────

def build_delivery_to_order_map(data_dir: str) -> dict[str, str]:
    """
    delivery_id → order_id

    outbound_delivery_headers has no order reference; the link lives in
    outbound_delivery_items (referenceSdDocument = salesOrder).
    We take the first order seen per delivery (one delivery = one order).
    """
    mapping: dict[str, str] = {}
    source = resolve_source(
        data_dir,
        "outbound_delivery_items.jsonl",
        "outbound_delivery_items",
    )
    for row in iter_jsonl(source):
        did = row.get("deliveryDocument", "")
        oid = row.get("referenceSdDocument", "")
        if did and oid and did not in mapping:
            mapping[did] = oid
    return mapping


def build_billing_to_delivery_map(data_dir: str) -> dict[str, str]:
    """
    billing_id → delivery_id

    billing_document_headers has no delivery reference; the link lives in
    billing_document_items (referenceSdDocument = deliveryDocument).
    We take the first delivery seen per billing document.
    """
    mapping: dict[str, str] = {}
    source = resolve_source(
        data_dir,
        "billing_document_items.jsonl",
        "billing_document_items",
    )
    for row in iter_jsonl(source):
        bid = row.get("billingDocument", "")
        did = row.get("referenceSdDocument", "")
        if bid and did and bid not in mapping:
            mapping[bid] = did
    return mapping


# ── Per-table loaders ─────────────────────────────────────────────────────────

def load_customers(cur: sqlite3.Cursor, data_dir: str) -> int:
    """business_partners → customers"""
    source = resolve_source(data_dir, "business_partners.jsonl", "business_partners")
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        cid = strip_zeros(row.get("customer") or row.get("businessPartner"))
        if not cid or cid in seen:
            continue
        seen.add(cid)
        name = (
            row.get("businessPartnerFullName")
            or row.get("businessPartnerName")
            or ""
        ).strip()
        cur.execute(
            "INSERT OR IGNORE INTO customers (id, name) VALUES (?, ?)",
            (cid, name),
        )
        count += 1
    return count


def load_products(cur: sqlite3.Cursor, data_dir: str) -> int:
    """products → products"""
    source = resolve_source(data_dir, "products.jsonl", "products")
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        pid = row.get("product", "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        cur.execute(
            "INSERT OR IGNORE INTO products (id, base_unit, product_type) VALUES (?, ?, ?)",
            (pid, row.get("baseUnit"), row.get("productType")),
        )
        count += 1
    return count


def load_product_descriptions(cur: sqlite3.Cursor, data_dir: str) -> int:
    """product_texts / product_descriptions → product_descriptions"""
    source = resolve_source(
        data_dir,
        "product_texts.jsonl",           # name used in task spec
        "product_descriptions.jsonl",    # actual dataset name
        "product_descriptions",          # folder fallback
    )
    seen: set[tuple[str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        pid = row.get("product", "").strip()
        lang = (row.get("language") or "EN").strip()
        if not pid:
            continue
        key = (pid, lang)
        if key in seen:
            continue
        seen.add(key)
        # Ensure parent product row exists (description may reference products
        # not present in the products.jsonl extract).
        cur.execute("INSERT OR IGNORE INTO products (id) VALUES (?)", (pid,))
        cur.execute(
            """
            INSERT OR IGNORE INTO product_descriptions (product_id, language, name)
            VALUES (?, ?, ?)
            """,
            (pid, lang, row.get("productDescription")),
        )
        count += 1
    return count


def load_orders(cur: sqlite3.Cursor, data_dir: str) -> int:
    """sales_order_headers → orders"""
    source = resolve_source(
        data_dir, "sales_order_headers.jsonl", "sales_order_headers"
    )
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        oid = row.get("salesOrder", "").strip()
        if not oid or oid in seen:
            continue
        seen.add(oid)
        customer_id = strip_zeros(row.get("soldToParty"))
        cur.execute(
            """
            INSERT OR IGNORE INTO orders (
                id, customer_id, creation_date, total_net_amount,
                currency, delivery_status, payment_terms, requested_delivery_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                oid,
                customer_id,
                row.get("creationDate"),
                to_float(row.get("totalNetAmount")),
                row.get("transactionCurrency"),
                row.get("overallDeliveryStatus"),
                row.get("customerPaymentTerms"),
                row.get("requestedDeliveryDate"),
            ),
        )
        count += 1
    return count


def load_order_items(cur: sqlite3.Cursor, data_dir: str) -> int:
    """sales_order_items → order_items"""
    source = resolve_source(
        data_dir, "sales_order_items.jsonl", "sales_order_items"
    )
    seen: set[tuple[str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        order_id = row.get("salesOrder", "").strip()
        # salesOrderItem comes as "10", "20" (no padding) in headers
        # but "000010" in delivery items — normalise for safety
        item_id = strip_zeros(row.get("salesOrderItem"))
        if not order_id or not item_id:
            continue
        key = (order_id, item_id)
        if key in seen:
            continue
        seen.add(key)
        product_id = row.get("material", "").strip() or None
        if product_id:
            cur.execute("INSERT OR IGNORE INTO products (id) VALUES (?)", (product_id,))
        cur.execute(
            """
            INSERT OR IGNORE INTO order_items (
                order_id, item_id, product_id,
                quantity, quantity_unit, net_amount, currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                item_id,
                product_id,
                to_float(row.get("requestedQuantity")),
                row.get("requestedQuantityUnit"),
                to_float(row.get("netAmount")),
                row.get("transactionCurrency"),
            ),
        )
        count += 1
    return count


def load_deliveries(
    cur: sqlite3.Cursor,
    data_dir: str,
    delivery_to_order: dict[str, str],
) -> int:
    """
    outbound_delivery_headers → deliveries

    order_id is resolved via *delivery_to_order* (built from delivery items)
    because delivery headers carry no order reference of their own.
    """
    source = resolve_source(
        data_dir,
        "outbound_delivery_headers.jsonl",
        "outbound_delivery_headers",
    )
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        did = row.get("deliveryDocument", "").strip()
        if not did or did in seen:
            continue
        seen.add(did)
        order_id = delivery_to_order.get(did)
        cur.execute(
            """
            INSERT OR IGNORE INTO deliveries (
                id, order_id, creation_date,
                goods_movement_status, picking_status
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                did,
                order_id,
                row.get("creationDate"),
                row.get("overallGoodsMovementStatus"),
                row.get("overallPickingStatus"),
            ),
        )
        count += 1
    return count


def load_billing(
    cur: sqlite3.Cursor,
    data_dir: str,
    billing_to_delivery: dict[str, str],
) -> int:
    """
    billing_document_headers + billing_document_cancellations → billing

    Both files share the same schema.  Cancellations are merged with
    is_cancelled=1.  delivery_id is resolved via *billing_to_delivery*
    (built from billing items).
    """
    sources = [
        (
            resolve_source(
                data_dir,
                "billing_document_headers.jsonl",
                "billing_document_headers",
            ),
            False,   # is_cancelled override
        ),
        (
            resolve_source(
                data_dir,
                "billing_document_cancellations.jsonl",
                "billing_document_cancellations",
            ),
            True,
        ),
    ]
    seen: set[str] = set()
    count = 0
    for source, force_cancelled in sources:
        for row in iter_jsonl(source):
            bid = row.get("billingDocument", "").strip()
            if not bid or bid in seen:
                continue
            seen.add(bid)
            customer_id = strip_zeros(row.get("soldToParty"))
            delivery_id = billing_to_delivery.get(bid)
            is_cancelled = (
                1
                if force_cancelled or row.get("billingDocumentIsCancelled")
                else 0
            )
            cur.execute(
                """
                INSERT OR IGNORE INTO billing (
                    id, delivery_id, customer_id, billing_date,
                    total_net_amount, currency, is_cancelled, accounting_document
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bid,
                    delivery_id,
                    customer_id,
                    row.get("billingDocumentDate") or row.get("creationDate"),
                    to_float(row.get("totalNetAmount")),
                    row.get("transactionCurrency"),
                    is_cancelled,
                    row.get("accountingDocument"),
                ),
            )
            count += 1
    return count


def load_payments(cur: sqlite3.Cursor, data_dir: str) -> int:
    """payments_accounts_receivable → payments"""
    source = resolve_source(
        data_dir,
        "payments_accounts_receivable.jsonl",
        "payments_accounts_receivable",
    )
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        pid = row.get("accountingDocument", "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        customer_id = strip_zeros(row.get("customer"))
        cur.execute(
            """
            INSERT OR IGNORE INTO payments (
                id, customer_id, amount, currency,
                posting_date, clearing_date, clearing_document, fiscal_year
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pid,
                customer_id,
                to_float(row.get("amountInTransactionCurrency")),
                row.get("transactionCurrency"),
                row.get("postingDate"),
                row.get("clearingDate"),
                row.get("clearingAccountingDocument"),
                row.get("fiscalYear"),
            ),
        )
        count += 1
    return count


def load_journal_entries(cur: sqlite3.Cursor, data_dir: str) -> int:
    """journal_entry_items_accounts_receivable → journal_entries"""
    source = resolve_source(
        data_dir,
        "journal_entry_items_accounts_receivable.jsonl",
        "journal_entry_items_accounts_receivable",
    )
    count = 0
    for row in iter_jsonl(source):
        acct_doc = row.get("accountingDocument", "").strip()
        if not acct_doc:
            continue
        customer_id = strip_zeros(row.get("customer"))
        cur.execute(
            """
            INSERT INTO journal_entries (
                accounting_document, billing_document, customer_id,
                gl_account, amount, currency,
                posting_date, document_type, profit_center
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                acct_doc,
                row.get("referenceDocument"),   # SAP referenceDocument = billingDocument
                customer_id,
                row.get("glAccount"),
                to_float(row.get("amountInTransactionCurrency")),
                row.get("transactionCurrency"),
                row.get("postingDate"),
                row.get("accountingDocumentType"),
                row.get("profitCenter"),
            ),
        )
        count += 1
    return count


def load_customer_company_assignments(cur: sqlite3.Cursor, data_dir: str) -> int:
    """customer_company_assignments → customer_company_assignments"""
    source = resolve_source(
        data_dir,
        "customer_company_assignments.jsonl",
        "customer_company_assignments",
    )
    seen: set[tuple[str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        customer_id = strip_zeros(row.get("customer"))
        company_code = (row.get("companyCode") or "").strip()
        if not customer_id or not company_code:
            continue
        key = (customer_id, company_code)
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            """
            INSERT OR IGNORE INTO customer_company_assignments (
                customer_id, company_code, reconciliation_account,
                payment_terms, customer_account_group, deletion_indicator
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                company_code,
                row.get("reconciliationAccount"),
                row.get("paymentTerms"),
                row.get("customerAccountGroup"),
                1 if row.get("deletionIndicator") else 0,
            ),
        )
        count += 1
    return count


def load_customer_sales_area_assignments(cur: sqlite3.Cursor, data_dir: str) -> int:
    """customer_sales_area_assignments → customer_sales_area_assignments"""
    source = resolve_source(
        data_dir,
        "customer_sales_area_assignments.jsonl",
        "customer_sales_area_assignments",
    )
    seen: set[tuple[str, str, str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        customer_id = strip_zeros(row.get("customer"))
        sales_org = (row.get("salesOrganization") or "").strip()
        dist_channel = (row.get("distributionChannel") or "").strip()
        division = (row.get("division") or "").strip()
        if not customer_id or not sales_org or not dist_channel or not division:
            continue
        key = (customer_id, sales_org, dist_channel, division)
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            """
            INSERT OR IGNORE INTO customer_sales_area_assignments (
                customer_id, sales_organization, distribution_channel, division,
                currency, customer_payment_terms, delivery_priority,
                incoterms_classification, incoterms_location1,
                shipping_condition, supplying_plant, exchange_rate_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                customer_id,
                sales_org,
                dist_channel,
                division,
                row.get("currency"),
                row.get("customerPaymentTerms"),
                row.get("deliveryPriority"),
                row.get("incotermsClassification"),
                row.get("incotermsLocation1"),
                row.get("shippingCondition"),
                row.get("supplyingPlant"),
                row.get("exchangeRateType"),
            ),
        )
        count += 1
    return count


def load_business_partner_addresses(cur: sqlite3.Cursor, data_dir: str) -> int:
    """business_partner_addresses → business_partner_addresses"""
    source = resolve_source(
        data_dir,
        "business_partner_addresses.jsonl",
        "business_partner_addresses",
    )
    seen: set[tuple[str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        bp_id = strip_zeros(row.get("businessPartner"))
        address_id = (row.get("addressId") or "").strip()
        if not bp_id or not address_id:
            continue
        key = (bp_id, address_id)
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            """
            INSERT OR IGNORE INTO business_partner_addresses (
                business_partner_id, address_id, city_name, country,
                postal_code, region, street_name, address_time_zone,
                validity_start_date, validity_end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bp_id,
                address_id,
                row.get("cityName"),
                row.get("country"),
                row.get("postalCode"),
                row.get("region"),
                row.get("streetName"),
                row.get("addressTimeZone"),
                row.get("validityStartDate"),
                row.get("validityEndDate"),
            ),
        )
        count += 1
    return count


def load_plants(cur: sqlite3.Cursor, data_dir: str) -> int:
    """plants → plants"""
    source = resolve_source(data_dir, "plants.jsonl", "plants")
    seen: set[str] = set()
    count = 0
    for row in iter_jsonl(source):
        plant_id = (row.get("plant") or "").strip()
        if not plant_id or plant_id in seen:
            continue
        seen.add(plant_id)
        cur.execute(
            """
            INSERT OR IGNORE INTO plants (
                plant_id, plant_name, sales_organization, distribution_channel,
                division, address_id, language, is_marked_for_archiving
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                plant_id,
                row.get("plantName"),
                row.get("salesOrganization"),
                row.get("distributionChannel"),
                row.get("division"),
                row.get("addressId"),
                row.get("language"),
                1 if row.get("isMarkedForArchiving") else 0,
            ),
        )
        count += 1
    return count


def load_product_plants(cur: sqlite3.Cursor, data_dir: str) -> int:
    """product_plants → product_plants"""
    source = resolve_source(data_dir, "product_plants.jsonl", "product_plants")
    seen: set[tuple[str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        product_id = (row.get("product") or "").strip()
        plant_id = (row.get("plant") or "").strip()
        if not product_id or not plant_id:
            continue
        key = (product_id, plant_id)
        if key in seen:
            continue
        seen.add(key)
        cur.execute("INSERT OR IGNORE INTO products (id) VALUES (?)", (product_id,))
        cur.execute("INSERT OR IGNORE INTO plants (plant_id) VALUES (?)", (plant_id,))
        cur.execute(
            """
            INSERT OR IGNORE INTO product_plants (
                product_id, plant_id, profit_center,
                availability_check_type, mrp_type
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                product_id,
                plant_id,
                row.get("profitCenter"),
                row.get("availabilityCheckType"),
                row.get("mrpType"),
            ),
        )
        count += 1
    return count


def load_product_storage_locations(cur: sqlite3.Cursor, data_dir: str) -> int:
    """product_storage_locations → product_storage_locations"""
    source = resolve_source(
        data_dir,
        "product_storage_locations.jsonl",
        "product_storage_locations",
    )
    seen: set[tuple[str, str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        product_id = (row.get("product") or "").strip()
        plant_id = (row.get("plant") or "").strip()
        storage_location = (row.get("storageLocation") or "").strip()
        if not product_id or not plant_id or not storage_location:
            continue
        key = (product_id, plant_id, storage_location)
        if key in seen:
            continue
        seen.add(key)
        cur.execute("INSERT OR IGNORE INTO products (id) VALUES (?)", (product_id,))
        cur.execute("INSERT OR IGNORE INTO plants (plant_id) VALUES (?)", (plant_id,))
        cur.execute(
            """
            INSERT OR IGNORE INTO product_storage_locations (
                product_id, plant_id, storage_location,
                physical_inventory_block_ind, date_of_last_posted_count
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                product_id,
                plant_id,
                storage_location,
                row.get("physicalInventoryBlockInd"),
                row.get("dateOfLastPostedCntUnRstrcdStk"),
            ),
        )
        count += 1
    return count


def load_sales_order_schedule_lines(cur: sqlite3.Cursor, data_dir: str) -> int:
    """sales_order_schedule_lines → sales_order_schedule_lines"""
    source = resolve_source(
        data_dir,
        "sales_order_schedule_lines.jsonl",
        "sales_order_schedule_lines",
    )
    seen: set[tuple[str, str, str]] = set()
    count = 0
    for row in iter_jsonl(source):
        sales_order_id = (row.get("salesOrder") or "").strip()
        sales_order_item_id = strip_zeros(row.get("salesOrderItem"))
        schedule_line = strip_zeros(row.get("scheduleLine"))
        if not sales_order_id or not sales_order_item_id or not schedule_line:
            continue
        key = (sales_order_id, sales_order_item_id, schedule_line)
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            """
            INSERT OR IGNORE INTO sales_order_schedule_lines (
                sales_order_id, sales_order_item_id, schedule_line,
                confirmed_delivery_date, order_quantity_unit, confirmed_order_qty
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                sales_order_id,
                sales_order_item_id,
                schedule_line,
                row.get("confirmedDeliveryDate"),
                row.get("orderQuantityUnit"),
                to_float(row.get("confdOrderQtyByMatlAvailCheck")),
            ),
        )
        count += 1
    return count


# ── Row-count reporter ────────────────────────────────────────────────────────

def print_row_counts(conn: sqlite3.Connection) -> None:
    tables = [
        "customers",
        "products",
        "product_descriptions",
        "orders",
        "order_items",
        "deliveries",
        "billing",
        "payments",
        "journal_entries",
        "customer_company_assignments",
        "customer_sales_area_assignments",
        "business_partner_addresses",
        "plants",
        "product_plants",
        "product_storage_locations",
        "sales_order_schedule_lines",
    ]
    print("\n── Row counts ──────────────────────────")
    for table in tables:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table:<28} {n:>6}")
    print("────────────────────────────────────────")


# ── Main entry point ──────────────────────────────────────────────────────────

def ingest(db_path: str = DB_PATH, data_dir: str = DATA_DIR) -> None:
    """
    Load all O2C JSONL source files into *db_path*.

    Insert order respects foreign-key dependencies:
      customers → products → product_descriptions
      → orders → order_items
      → deliveries → billing → payments → journal_entries
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Database not found: {db_path!r}. Run setup_db.py first."
        )
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(
            f"Data directory not found: {data_dir!r}"
        )

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        cur = conn.cursor()

        # Build cross-file lookup maps before inserting so FKs resolve correctly
        print("Building lookup maps …")
        delivery_to_order = build_delivery_to_order_map(data_dir)
        billing_to_delivery = build_billing_to_delivery_map(data_dir)
        print(f"  delivery→order  : {len(delivery_to_order)} entries")
        print(f"  billing→delivery: {len(billing_to_delivery)} entries")

        # Insert in FK-safe order
        steps = [
            ("customers",            lambda c: load_customers(c, data_dir)),
            ("products",             lambda c: load_products(c, data_dir)),
            ("product_descriptions", lambda c: load_product_descriptions(c, data_dir)),
            ("plants",               lambda c: load_plants(c, data_dir)),
            ("product_plants",       lambda c: load_product_plants(c, data_dir)),
            ("product_storage_locations", lambda c: load_product_storage_locations(c, data_dir)),
            ("orders",               lambda c: load_orders(c, data_dir)),
            ("order_items",          lambda c: load_order_items(c, data_dir)),
            ("sales_order_schedule_lines", lambda c: load_sales_order_schedule_lines(c, data_dir)),
            ("deliveries",           lambda c: load_deliveries(c, data_dir, delivery_to_order)),
            ("billing",              lambda c: load_billing(c, data_dir, billing_to_delivery)),
            ("payments",             lambda c: load_payments(c, data_dir)),
            ("journal_entries",      lambda c: load_journal_entries(c, data_dir)),
            ("customer_company_assignments", lambda c: load_customer_company_assignments(c, data_dir)),
            ("customer_sales_area_assignments", lambda c: load_customer_sales_area_assignments(c, data_dir)),
            ("business_partner_addresses", lambda c: load_business_partner_addresses(c, data_dir)),
        ]

        print("\nIngesting …")
        for label, loader in steps:
            n = loader(cur)
            print(f"  {label:<28} {n:>6} rows inserted")

        conn.commit()
        print_row_counts(conn)
        print("\nDatabase schema created successfully")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    ingest()
