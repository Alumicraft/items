"""
Frappe whitelisted API methods for the Items Manager page.
All methods require an authenticated session (@frappe.whitelist() without allow_guest).
"""

import frappe
from items.csv_parser import parse_csv, build_item_doc, validate_missing, NAMING_SERIES


@frappe.whitelist()
def validate_csv(file_content: str) -> dict:
    """
    Parse the CSV and check ERPNext for missing Item Groups, UOMs, and Suppliers.
    Returns: { ready: N, missing: { item_groups: [], uoms: [], suppliers: [] } }
    """
    rows = parse_csv(file_content)

    required_groups = {r["item_group"] for r in rows}
    required_uoms = {r["stock_uom"] for r in rows}
    required_suppliers = {r["supplier"] for r in rows if r.get("supplier")}

    existing_groups = set(
        frappe.get_list("Item Group", filters={"name": ["in", list(required_groups)]}, pluck="name")
    )
    existing_uoms = set(
        frappe.get_list("UOM", filters={"name": ["in", list(required_uoms)]}, pluck="name")
    )
    existing_suppliers = set(
        frappe.get_list("Supplier", filters={"name": ["in", list(required_suppliers)]}, pluck="name")
    )

    return {
        "ready": len(rows),
        "missing": {
            "item_groups": sorted(validate_missing(required_groups, existing_groups)),
            "uoms": sorted(validate_missing(required_uoms, existing_uoms)),
            "suppliers": sorted(validate_missing(required_suppliers, existing_suppliers)),
        },
    }


@frappe.whitelist()
def enqueue_import(file_content: str) -> dict:
    """
    Enqueue the import as a background job. Returns job_id immediately.
    Frontend polls get_progress(job_id) every 2s to track progress.
    """
    job_id = frappe.generate_hash(length=12)
    frappe.enqueue(
        "items.api.import_items",
        queue="long",
        file_content=file_content,
        _job_id=job_id,
    )
    return {"job_id": job_id}


def import_items(file_content: str, _job_id: str) -> None:
    """
    Background worker. Parses CSV and inserts items one-by-one.
    Per-row errors are logged; the batch continues regardless.
    Called by frappe.enqueue — NOT whitelisted, not callable from the frontend.
    """
    rows = parse_csv(file_content)
    total = len(rows)
    imported = 0
    skipped = 0
    errors = []

    for i, row in enumerate(rows):
        try:
            doc_dict = build_item_doc(row)
            # Skip items that already exist in ERPNext (re-import safe)
            if frappe.db.exists("Item", {"item_code": doc_dict["item_code"]}):
                skipped += 1
            else:
                doc = frappe.get_doc(doc_dict)
                doc.insert(ignore_permissions=True)
                imported += 1
        except Exception as e:
            errors.append({
                "row": i + 1,
                "item_code": row.get("item_code", ""),
                "error": str(e),
            })

        # Update progress in Redis cache every row
        frappe.cache().hset(_job_id, "progress", {
            "current": i + 1,
            "total": total,
            "imported": imported,
            "errors": errors,
        })

    frappe.db.commit()


@frappe.whitelist()
def get_progress(job_id: str) -> dict:
    """
    Returns current import progress from Redis cache.
    Returns zeroed state if job not found.
    """
    return frappe.cache().hget(job_id, "progress") or {
        "current": 0,
        "total": 0,
        "imported": 0,
        "errors": [],
    }


@frappe.whitelist()
def get_items(search: str = "", item_group: str = "", page: int = 1, page_size: int = 50) -> dict:
    """
    Returns paginated item list with supplier (joined from child table).
    """
    page = int(page)
    page_size = int(page_size)
    offset = (page - 1) * page_size

    sql = """
        SELECT i.name, i.item_name, i.item_group, i.stock_uom,
               GROUP_CONCAT(s.supplier SEPARATOR ', ') AS supplier
        FROM `tabItem` i
        LEFT JOIN `tabItem Supplier` s ON s.parent = i.name
        WHERE (%s = '' OR i.item_name LIKE %s)
          AND (%s = '' OR i.item_group = %s)
        GROUP BY i.name
        ORDER BY i.item_name
        LIMIT %s OFFSET %s
    """
    rows = frappe.db.sql(
        sql,
        (search, f"%{search}%", item_group, item_group, page_size, offset),
        as_dict=True,
    )

    count_filters = {}
    if search:
        count_filters["item_name"] = ["like", f"%{search}%"]
    if item_group:
        count_filters["item_group"] = item_group
    total = frappe.db.count("Item", filters=count_filters)

    return {"items": rows, "total": total}


@frappe.whitelist()
def delete_items(item_codes: list = None, delete_all: bool = False) -> dict:
    """
    Delete items from ERPNext atomically.
    - delete_all=True: fetches all item names first, then deletes everything
    - Linked-doc guard: refuses to delete items referenced in open Sales/Purchase Orders
    - Returns { deleted: N, protected: [...], error: null }
    """
    import json

    # Deserialize first — Frappe may deliver item_codes as a JSON string
    if isinstance(item_codes, str):
        item_codes = json.loads(item_codes)
    if isinstance(delete_all, str):
        delete_all = delete_all.lower() == "true"

    if delete_all:
        item_codes = frappe.get_list("Item", pluck="name")

    if not item_codes:
        return {"deleted": 0, "protected": [], "error": None}

    item_codes = list(item_codes)

    # Linked-document guard — do not delete items in open orders
    linked_rows = frappe.db.sql(
        """
        SELECT DISTINCT item_code FROM `tabSales Order Item`
        WHERE item_code IN %(codes)s AND docstatus < 2
        UNION
        SELECT DISTINCT item_code FROM `tabPurchase Order Item`
        WHERE item_code IN %(codes)s AND docstatus < 2
        """,
        {"codes": item_codes},
    )
    protected = [r[0] for r in linked_rows]
    if protected:
        return {
            "deleted": 0,
            "protected": protected,
            "error": f"{len(protected)} item(s) are referenced in open orders and cannot be deleted.",
        }

    # Atomic delete: child tables first, then parent
    frappe.db.begin()
    try:
        frappe.db.sql(
            "DELETE FROM `tabItem Supplier` WHERE parent IN %(codes)s",
            {"codes": item_codes},
        )
        frappe.db.sql(
            "DELETE FROM `tabItem Default` WHERE parent IN %(codes)s",
            {"codes": item_codes},
        )
        frappe.db.sql(
            "DELETE FROM `tabItem` WHERE name IN %(codes)s",
            {"codes": item_codes},
        )
        frappe.db.commit()
    except Exception as e:
        frappe.db.rollback()
        raise

    return {"deleted": len(item_codes), "protected": [], "error": None}


@frappe.whitelist()
def get_review_queue() -> list:
    """
    Reads review_queue.csv from the configured pipeline output path.
    Cross-references against ERPNext to mark which items exist.
    Returns dict with error key and rows list.
    """
    import csv
    import os

    path = frappe.conf.get("items_pipeline_output_path")
    if not path:
        # Return structured error so JS can display a friendly message
        return {"error": (
            "items_pipeline_output_path not configured. "
            "Run: bench --site <site> set-config items_pipeline_output_path /path/to/output"
        ), "rows": []}

    csv_path = os.path.join(path, "review_queue.csv")
    if not os.path.exists(csv_path):
        return {"error": f"review_queue.csv not found at {csv_path}", "rows": []}

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))

    # Batch-check which item_codes exist in ERPNext.
    # Use raw SQL to reliably query item_code (a regular field, not the primary key
    # when naming series is active — name = STO-ITEM-YYYY-XXXXX, item_code = canonical name).
    item_codes = [r["item_code"] for r in rows if r.get("item_code")]
    if item_codes:
        existing_rows = frappe.db.sql(
            "SELECT item_code FROM `tabItem` WHERE item_code IN %(codes)s",
            {"codes": item_codes},
        )
        existing = {r[0] for r in existing_rows}
    else:
        existing = set()

    for row in rows:
        row["exists_in_erpnext"] = row.get("item_code", "") in existing

    return {"error": None, "rows": rows}
