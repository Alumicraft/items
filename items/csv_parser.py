"""
Pure Python CSV parsing for ERPNext item_master.csv pipeline output.
No Frappe dependency — fully unit-testable.
"""

import csv
import io

NAMING_SERIES = "STO-ITEM-YYYY."
_SENTINEL = "Start entering data below this line"

# Column indices in data rows (0-indexed, matches pipeline output format)
_COL_ITEM_NAME = 2
_COL_ITEM_CODE = 3
_COL_ITEM_GROUP = 4
_COL_STOCK_UOM = 5
_COL_COMPANY = 9
_COL_SUPPLIER = 12


def _normalize(s):
    """Convert to ALL CAPS and strip whitespace."""
    if not s:
        return s
    return s.strip().upper()


def parse_csv(file_content: str) -> list[dict]:
    """
    Parse ERPNext Data Import Template CSV.
    Scans for sentinel row, returns data rows as dicts.
    Raises ValueError if sentinel not found.
    """
    reader = csv.reader(io.StringIO(file_content))
    rows = list(reader)

    # Find sentinel row
    sentinel_idx = None
    for i, row in enumerate(rows):
        if row and row[0].strip() == _SENTINEL:
            sentinel_idx = i
            break

    if sentinel_idx is None:
        raise ValueError(
            f"CSV sentinel '{_SENTINEL}' not found. "
            "Ensure this is pipeline output (item_master.csv)."
        )

    data_rows = rows[sentinel_idx + 1:]
    result = []

    for row in data_rows:
        # Skip empty rows and rows that don't have enough columns
        if not row or len(row) <= _COL_SUPPLIER:
            continue
        # Skip rows where item_name is blank (trailing empty lines)
        item_name = row[_COL_ITEM_NAME].strip()
        if not item_name:
            continue

        result.append({
            "item_name": item_name,
            "item_code": row[_COL_ITEM_CODE].strip(),
            "item_group": row[_COL_ITEM_GROUP].strip(),
            "stock_uom": row[_COL_STOCK_UOM].strip(),
            "company": row[_COL_COMPANY].strip(),
            "supplier": row[_COL_SUPPLIER].strip(),
        })

    return result


def build_item_doc(row: dict) -> dict:
    """
    Build an ERPNext Item doc dict from a parsed CSV row.
    Applies ALL CAPS normalization to name fields.
    """
    item_name = _normalize(row["item_name"])
    item_code = _normalize(row["item_code"])
    description = _normalize(row.get("description", ""))
    is_stock = 0 if row["item_group"] == "Service" else 1

    doc = {
        "doctype": "Item",
        "naming_series": NAMING_SERIES,
        "item_name": item_name,
        "item_code": item_code,
        "item_group": row["item_group"],
        "stock_uom": row["stock_uom"],
        "is_stock_item": is_stock,
        "item_defaults": [{"company": row["company"]}],
        "supplier_items": (
            [{"supplier": row["supplier"]}] if row.get("supplier") else []
        ),
    }
    if description:
        doc["description"] = description
    return doc


def validate_missing(required: set, existing: set) -> set:
    """
    Return the set of required values not present in existing.
    Pure set difference — no Frappe dependency.
    """
    return required - existing
