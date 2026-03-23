"""
Frappe whitelisted API methods for the Items Manager page.
All methods require an authenticated session (@frappe.whitelist() without allow_guest).
"""

import frappe
from items.csv_parser import parse_csv, build_item_doc, validate_missing


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
            if frappe.db.exists("Item", {"item_name": doc_dict["item_name"]}):
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
    - Linked-doc guard: skips items with any connections (orders, invoices, tax templates, etc.)
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

    # Linked-document guard — skip items referenced in ANY transaction table.
    # Dynamic Link doesn't cover ERPNext child tables, so we check each one directly.
    linked_tables = [
        ("tabSales Order Item", "item_code"),
        ("tabPurchase Order Item", "item_code"),
        ("tabSales Invoice Item", "item_code"),
        ("tabPurchase Invoice Item", "item_code"),
        ("tabDelivery Note Item", "item_code"),
        ("tabPurchase Receipt Item", "item_code"),
        ("tabStock Entry Detail", "item_code"),
        ("tabQuotation Item", "item_code"),
        ("tabSupplier Quotation Item", "item_code"),
        ("tabMaterial Request Item", "item_code"),
        ("tabBOM", "item"),
        ("tabBOM Item", "item_code"),
        ("tabStock Ledger Entry", "item_code"),
        ("tabItem Price", "item_code"),
        ("tabItem Tax", "parent"),
        ("tabPacked Item", "item_code"),
    ]

    protected = set()
    for table, col in linked_tables:
        try:
            rows = frappe.db.sql(
                f"SELECT DISTINCT `{col}` FROM `{table}` WHERE `{col}` IN %(codes)s",
                {"codes": item_codes},
            )
            protected.update(r[0] for r in rows)
        except Exception:
            # Table may not exist in this ERPNext setup — skip it
            pass

    protected = list(protected)
    deletable = [code for code in item_codes if code not in protected]

    if not deletable:
        return {
            "deleted": 0,
            "protected": protected,
            "error": f"All {len(protected)} item(s) have connections and cannot be deleted." if protected else None,
        }

    # Atomic delete: child tables first, then parent
    frappe.db.begin()
    try:
        frappe.db.sql(
            "DELETE FROM `tabItem Supplier` WHERE parent IN %(codes)s",
            {"codes": deletable},
        )
        frappe.db.sql(
            "DELETE FROM `tabItem Default` WHERE parent IN %(codes)s",
            {"codes": deletable},
        )
        frappe.db.sql(
            "DELETE FROM `tabItem` WHERE name IN %(codes)s",
            {"codes": deletable},
        )
        frappe.db.commit()
    except Exception as e:
        frappe.db.rollback()
        raise

    return {"deleted": len(deletable), "protected": protected, "error": None}


# ── Part number splitting (mirrors pipeline/stage8_validate.py patterns) ──
import re

_PART_NUM_PATTERNS = [
    re.compile(r'^(PRM-[A-Z0-9-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(CL-[A-Z]+-[A-Z0-9]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(BHP-[A-Z0-9]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(BRG-\d+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(FK-[A-Z0-9-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(FKB\s+[A-Z0-9]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(H-AN[0-9A-Z-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(ASC-\d{3}[A-Z0-9]*)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(KTK[\s-][A-Z0-9-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(IDI\d+-\d+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(XRP\d+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(MTX\d+[A-Z]*)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(SPR\s+\d+[A-Z0-9]*)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(ORW\d+-\d+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(MS[0-9]{4,}-[0-9]+)(?:\s+(.+))?$', re.IGNORECASE),
    re.compile(r'^(\d{4,}[A-Z]\d{2,})\s+(.+)', re.IGNORECASE),
    re.compile(r'^(PP\d+[A-Z0-9.-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^([A-Z]{1,5}\d+[A-Z0-9]*-[A-Z0-9-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^([A-Z]+\d+[A-Z]+\d*[A-Z]*)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(\d+-[A-Z][A-Z0-9]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(\d{3}-\d{2,}-[\w-]+)\s+(.+)', re.IGNORECASE),
    re.compile(r'^(\d{3}-\d{2,}-[\w-]+)$', re.IGNORECASE),
    re.compile(r'^(\d{3,}-\d{3,})(?:\s+-?\s*(.+))?$', re.IGNORECASE),
    re.compile(r'^(\d{3,}-\d{3,})$', re.IGNORECASE),
]


def _split_part_number(name: str) -> tuple:
    """Split a part number prefix from an item name.
    Returns (clean_name, supplier_part_no).
    """
    for pattern in _PART_NUM_PATTERNS:
        m = pattern.match(name)
        if m:
            part_num = m.group(1).strip()
            description = (m.group(2) or "").strip().lstrip("- ").strip()
            if not description:
                return (name, part_num)
            return (description, part_num)
    return (name, "")


def _load_pipeline_item_groups() -> dict:
    """Load item_group mapping from pipeline item_master.csv.

    Returns dict mapping uppercased item_name → item_group.
    Reads from the same configured output path as review_queue.
    """
    import csv
    import os

    path = frappe.conf.get("items_pipeline_output_path")
    if not path:
        return {}

    csv_path = os.path.join(path, "item_master.csv")
    if not os.path.exists(csv_path):
        return {}

    group_map = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Find sentinel row
    sentinel_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == "Start entering data below this line":
            sentinel_idx = i
            break

    if sentinel_idx is None:
        return {}

    for row in rows[sentinel_idx + 1:]:
        if len(row) >= 5:
            item_name = row[2].strip().upper()
            item_group = row[4].strip()
            supplier_part_no = row[13].strip() if len(row) > 13 else ""
            if item_name and item_group:
                # Map by current clean name
                group_map[item_name] = item_group
                # Also map by reconstructed old name (part_no + name)
                # so we can match items imported before the split
                if supplier_part_no:
                    old_name = f"{supplier_part_no} {item_name}".upper()
                    group_map[old_name] = item_group

    return group_map


@frappe.whitelist()
def cleanup_item_names() -> dict:
    """
    One-time cleanup for existing items:
    - Splits part numbers out of item_name into supplier_part_no
    - Fixes item_group using pipeline data (for items stuck in wrong group)
    - Leaves item_code untouched so existing links stay intact
    Returns { updated: N, skipped: N, details: [...] }
    """
    items = frappe.get_all(
        "Item",
        fields=["name", "item_name", "item_code", "item_group"],
        limit_page_length=0,
    )

    # Load pipeline group mapping
    pipeline_groups = _load_pipeline_item_groups()

    updated = 0
    skipped = 0
    details = []

    for item in items:
        old_name = item.get("item_name") or ""
        clean_name, part_no = _split_part_number(old_name)
        changed = False

        doc = None

        # Fix item_name if part number was split out
        if part_no and clean_name != old_name:
            doc = doc or frappe.get_doc("Item", item["name"])
            doc.item_name = clean_name.upper()[:140]
            changed = True

            # Add supplier_part_no to existing supplier rows
            if doc.supplier_items:
                for supplier_row in doc.supplier_items:
                    if not supplier_row.supplier_part_no:
                        supplier_row.supplier_part_no = part_no

        # Fix item_group from pipeline data — try multiple name formats
        lookup_name = old_name.upper()
        pipeline_group = pipeline_groups.get(lookup_name)
        if not pipeline_group and clean_name != old_name:
            pipeline_group = pipeline_groups.get(clean_name.upper())
        # Try normalized lookup (strip extra spaces/punctuation)
        if not pipeline_group:
            normalized = re.sub(r'[^\w\s]', '', lookup_name).strip()
            normalized = re.sub(r'\s+', ' ', normalized)
            for key, group in pipeline_groups.items():
                key_norm = re.sub(r'[^\w\s]', '', key).strip()
                key_norm = re.sub(r'\s+', ' ', key_norm)
                if key_norm == normalized:
                    pipeline_group = group
                    break
        if pipeline_group and pipeline_group != item.get("item_group"):
            doc = doc or frappe.get_doc("Item", item["name"])
            doc.item_group = pipeline_group
            changed = True

        if changed:
            doc.save(ignore_permissions=True)
            updated += 1
            details.append({
                "item_code": item["item_code"],
                "old_name": old_name,
                "new_name": doc.item_name,
                "old_group": item.get("item_group"),
                "new_group": doc.item_group,
                "supplier_part_no": part_no or "",
            })
        else:
            skipped += 1

    frappe.db.commit()

    return {"updated": updated, "skipped": skipped, "details": details}


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
