app_name = "items"
app_title = "Items"
app_publisher = "Alumicraft"
app_description = "Item master import manager for ERPNext"
app_email = "admin@alumicraft.com"
app_license = "MIT"
app_version = "0.1.0"

doc_events = {
    "Item": {
        "before_validate": "items.overrides.item.before_validate"
    }
}

after_migrate = ["items.overrides.item.set_item_code_not_mandatory"]
