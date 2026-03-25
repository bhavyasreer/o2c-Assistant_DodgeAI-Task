import sqlite3
import os

DB_PATH = "data.db"

DDL_STATEMENTS = [
    # ── Dimension tables ──────────────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS customers (
        id    TEXT PRIMARY KEY,  -- SAP: business_partners.customer (leading zeros stripped)
        name  TEXT NOT NULL      -- SAP: businessPartnerFullName
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS products (
        id            TEXT PRIMARY KEY,  -- SAP: products.product
        base_unit     TEXT,              -- e.g. "PC"
        product_type  TEXT               -- e.g. "ZPKG"
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS product_descriptions (
        product_id  TEXT NOT NULL REFERENCES products(id),
        language    TEXT NOT NULL DEFAULT 'EN',  -- SAP: language
        name        TEXT,                         -- SAP: productDescription
        PRIMARY KEY (product_id, language)
    )
    """,

    # ── O2C flow tables ───────────────────────────────────────────────────

    """
    CREATE TABLE IF NOT EXISTS orders (
        id                      TEXT PRIMARY KEY,  -- SAP: salesOrder
        customer_id             TEXT NOT NULL REFERENCES customers(id),  -- SAP: soldToParty
        creation_date           TEXT,              -- ISO datetime
        total_net_amount        REAL,
        currency                TEXT,              -- e.g. "INR"
        delivery_status         TEXT,              -- SAP: overallDeliveryStatus
        payment_terms           TEXT,              -- SAP: customerPaymentTerms
        requested_delivery_date TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS order_items (
        order_id       TEXT NOT NULL REFERENCES orders(id),   -- SAP: salesOrder
        item_id        TEXT NOT NULL,                          -- SAP: salesOrderItem (leading zeros stripped)
        product_id     TEXT REFERENCES products(id),           -- SAP: material
        quantity       REAL,                                   -- SAP: requestedQuantity
        quantity_unit  TEXT,
        net_amount     REAL,
        currency       TEXT,
        PRIMARY KEY (order_id, item_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id                    TEXT PRIMARY KEY,  -- SAP: deliveryDocument
        order_id              TEXT REFERENCES orders(id),  -- SAP: outbound_delivery_items.referenceSdDocument
        creation_date         TEXT,
        goods_movement_status TEXT,  -- SAP: overallGoodsMovementStatus
        picking_status        TEXT   -- SAP: overallPickingStatus
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS billing (
        id                  TEXT PRIMARY KEY,  -- SAP: billingDocument (90* = invoice, 91* = cancellation)
        delivery_id         TEXT REFERENCES deliveries(id),  -- SAP: billing_document_items.referenceSdDocument
        customer_id         TEXT REFERENCES customers(id),   -- SAP: soldToParty
        billing_date        TEXT,
        total_net_amount    REAL,
        currency            TEXT,
        is_cancelled        INTEGER DEFAULT 0,  -- 1 = cancelled billing doc
        accounting_document TEXT                -- links to journal_entries / payments
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS payments (
        id                TEXT PRIMARY KEY,  -- SAP: accountingDocument
        customer_id       TEXT REFERENCES customers(id),  -- SAP: customer
        amount            REAL,              -- SAP: amountInTransactionCurrency
        currency          TEXT,
        posting_date      TEXT,
        clearing_date     TEXT,
        clearing_document TEXT,              -- SAP: clearingAccountingDocument (joins to billing.accounting_document)
        fiscal_year       TEXT
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS journal_entries (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        accounting_document TEXT NOT NULL,            -- SAP: accountingDocument
        billing_document    TEXT REFERENCES billing(id),   -- SAP: referenceDocument
        customer_id         TEXT REFERENCES customers(id),
        gl_account          TEXT,                     -- SAP: glAccount
        amount              REAL,                     -- SAP: amountInTransactionCurrency
        currency            TEXT,
        posting_date        TEXT,
        document_type       TEXT,                     -- SAP: accountingDocumentType (e.g. "RV")
        profit_center       TEXT
    )
    """,

    # ── Supporting entities from raw SAP extracts ─────────────────────────

    """
    CREATE TABLE IF NOT EXISTS customer_company_assignments (
        customer_id            TEXT NOT NULL REFERENCES customers(id),
        company_code           TEXT NOT NULL,
        reconciliation_account TEXT,
        payment_terms          TEXT,
        customer_account_group TEXT,
        deletion_indicator     INTEGER DEFAULT 0,
        PRIMARY KEY (customer_id, company_code)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS customer_sales_area_assignments (
        customer_id            TEXT NOT NULL REFERENCES customers(id),
        sales_organization     TEXT NOT NULL,
        distribution_channel   TEXT NOT NULL,
        division               TEXT NOT NULL,
        currency               TEXT,
        customer_payment_terms TEXT,
        delivery_priority      TEXT,
        incoterms_classification TEXT,
        incoterms_location1    TEXT,
        shipping_condition     TEXT,
        supplying_plant        TEXT,
        exchange_rate_type     TEXT,
        PRIMARY KEY (customer_id, sales_organization, distribution_channel, division)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS business_partner_addresses (
        business_partner_id TEXT NOT NULL REFERENCES customers(id),
        address_id          TEXT NOT NULL,
        city_name           TEXT,
        country             TEXT,
        postal_code         TEXT,
        region              TEXT,
        street_name         TEXT,
        address_time_zone   TEXT,
        validity_start_date TEXT,
        validity_end_date   TEXT,
        PRIMARY KEY (business_partner_id, address_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS plants (
        plant_id             TEXT PRIMARY KEY,
        plant_name           TEXT,
        sales_organization   TEXT,
        distribution_channel TEXT,
        division             TEXT,
        address_id           TEXT,
        language             TEXT,
        is_marked_for_archiving INTEGER DEFAULT 0
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS product_plants (
        product_id            TEXT NOT NULL REFERENCES products(id),
        plant_id              TEXT NOT NULL REFERENCES plants(plant_id),
        profit_center         TEXT,
        availability_check_type TEXT,
        mrp_type              TEXT,
        PRIMARY KEY (product_id, plant_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS product_storage_locations (
        product_id        TEXT NOT NULL REFERENCES products(id),
        plant_id          TEXT NOT NULL REFERENCES plants(plant_id),
        storage_location  TEXT NOT NULL,
        physical_inventory_block_ind TEXT,
        date_of_last_posted_count TEXT,
        PRIMARY KEY (product_id, plant_id, storage_location)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS sales_order_schedule_lines (
        sales_order_id            TEXT NOT NULL REFERENCES orders(id),
        sales_order_item_id       TEXT NOT NULL,
        schedule_line             TEXT NOT NULL,
        confirmed_delivery_date   TEXT,
        order_quantity_unit       TEXT,
        confirmed_order_qty       REAL,
        PRIMARY KEY (sales_order_id, sales_order_item_id, schedule_line)
    )
    """,

    # ── Unified analytical view ───────────────────────────────────────────

    """
    CREATE VIEW IF NOT EXISTS v_order_to_cash AS
    SELECT
        -- Customer
        c.id                     AS customer_id,
        c.name                   AS customer_name,

        -- Order
        o.id                     AS order_id,
        o.creation_date          AS order_date,
        o.total_net_amount       AS order_total_amount,
        o.currency               AS order_currency,
        o.delivery_status        AS order_delivery_status,
        o.payment_terms,
        o.requested_delivery_date,

        -- Order Item
        oi.item_id               AS order_item_id,
        oi.quantity              AS ordered_quantity,
        oi.quantity_unit,
        oi.net_amount            AS item_net_amount,

        -- Product
        oi.product_id,
        COALESCE(pd.name, p.id)  AS product_name,
        p.base_unit,
        p.product_type,

        -- Delivery
        d.id                     AS delivery_id,
        d.creation_date          AS delivery_date,
        d.goods_movement_status,
        d.picking_status,

        -- Billing
        b.id                     AS billing_id,
        b.billing_date,
        b.total_net_amount       AS billed_amount,
        b.is_cancelled           AS billing_is_cancelled,
        b.accounting_document    AS billing_accounting_doc,

        -- Payment
        pay.id                   AS payment_id,
        pay.amount               AS payment_amount,
        pay.posting_date         AS payment_date,
        pay.clearing_date,

        -- Journal Entry
        je.accounting_document   AS journal_accounting_doc,
        je.billing_document      AS journal_billing_doc,
        je.gl_account,
        je.amount                AS journal_amount,
        je.posting_date          AS journal_posting_date,
        je.document_type         AS journal_doc_type,
        je.profit_center

    FROM orders o
    JOIN  customers c             ON c.id  = o.customer_id
    LEFT JOIN order_items oi      ON oi.order_id   = o.id
    LEFT JOIN products p          ON p.id           = oi.product_id
    LEFT JOIN product_descriptions pd
                                  ON pd.product_id  = p.id AND pd.language = 'EN'
    LEFT JOIN deliveries d        ON d.order_id     = o.id
    LEFT JOIN billing b           ON b.delivery_id  = d.id
    LEFT JOIN payments pay        ON pay.customer_id      = c.id
                                 AND pay.clearing_document = b.accounting_document
    LEFT JOIN journal_entries je  ON je.billing_document  = b.id
    """,
]


def setup_database(db_path: str = DB_PATH) -> None:
    """Create all O2C tables and the analytical view in db_path."""

    # Ensure the parent directory exists
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")

        for statement in DDL_STATEMENTS:
            conn.execute(statement)

        conn.commit()
        print("Database schema created successfully")

    except sqlite3.Error as exc:
        conn.rollback()
        raise RuntimeError(f"Schema creation failed: {exc}") from exc

    finally:
        conn.close()


if __name__ == "__main__":
    setup_database()
