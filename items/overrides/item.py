def before_validate(doc, method):
    if not doc.item_code and doc.item_name:
        doc.item_code = doc.item_name.upper()

    if doc.item_code:
        doc.item_code = doc.item_code.upper()
    if doc.item_name:
        doc.item_name = doc.item_name.upper()
    if doc.description:
        doc.description = doc.description.upper()
