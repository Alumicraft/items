import frappe


def before_validate(doc, method):
    if not doc.item_code and doc.item_name:
        doc.item_code = doc.item_name.upper()

    if doc.item_code:
        doc.item_code = doc.item_code.upper()
    if doc.item_name:
        doc.item_name = doc.item_name.upper()
    if doc.description:
        doc.description = doc.description.upper()


def set_item_code_not_mandatory():
    for prop, value in [("reqd", "0"), ("hidden", "0")]:
        if not frappe.db.exists("Property Setter", {
            "doc_type": "Item",
            "field_name": "item_code",
            "property": prop,
        }):
            ps = frappe.new_doc("Property Setter")
            ps.doctype_or_field = "DocField"
            ps.doc_type = "Item"
            ps.field_name = "item_code"
            ps.property = prop
            ps.value = value
            ps.property_type = "Check"
            ps.insert(ignore_permissions=True)

    frappe.db.commit()
