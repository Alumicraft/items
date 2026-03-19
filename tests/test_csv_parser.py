import pytest
from items.csv_parser import parse_csv, _normalize, build_item_doc, validate_missing

# Minimal CSV that matches the actual pipeline output format
SAMPLE_CSV = """\
"Data Import Template"
"Table:","Item"
""
""
"Notes:"
"Please do not change the template headings."
"First data column must be blank."
"If you are uploading new records, leave the ""name"" (ID) column blank."
"DocType:","Item","","","","","~","~","Item Default","item_defaults","~","Item Supplier","supplier_items"
"Column Labels:","ID","Item Name","Item Code","Item Group","Default Unit of Measure","","","ID","Company","","ID","Supplier"
"Column Name:","name","item_name","item_code","item_group","stock_uom","~","~","name","company","~","name","supplier"
"Mandatory:","Yes","Yes","Yes","Yes","Yes","","","Yes","Yes","","Yes","Yes"
"Type:","Data","Data","Data","Link","Link","","","Data","Link","","Data","Link"
"Info:","","","","Valid Item Group","Valid UOM","","","","Valid Company","","","Valid Supplier"
"Start entering data below this line"
"","","Hardware","Hardware","Part","Each","","","","Alumicraft","","","RPI, INC"
"","","DOM Tubing","DOM Tubing","Material","Foot","","","","Alumicraft","","","Competitive Metals"
"","","Labor - Install","Labor - Install","Service","Each","","","","Alumicraft","","",""
"""


class TestParseCsv:
    def test_returns_correct_row_count(self):
        rows = parse_csv(SAMPLE_CSV)
        assert len(rows) == 3

    def test_extracts_item_name(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["item_name"] == "Hardware"

    def test_extracts_item_code(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["item_code"] == "Hardware"

    def test_extracts_item_group(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["item_group"] == "Part"

    def test_extracts_stock_uom(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["stock_uom"] == "Each"

    def test_extracts_company(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["company"] == "Alumicraft"

    def test_extracts_supplier(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[0]["supplier"] == "RPI, INC"

    def test_empty_supplier_is_empty_string(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[2]["supplier"] == ""

    def test_skips_boilerplate_rows(self):
        rows = parse_csv(SAMPLE_CSV)
        names = [r["item_name"] for r in rows]
        assert "Start entering data below this line" not in names
        assert "Data Import Template" not in names

    def test_raises_on_missing_sentinel(self):
        with pytest.raises(ValueError, match="sentinel"):
            parse_csv("bad,csv,content\nno,sentinel,here\n")

    def test_foot_uom_row(self):
        rows = parse_csv(SAMPLE_CSV)
        assert rows[1]["stock_uom"] == "Foot"
        assert rows[1]["supplier"] == "Competitive Metals"


class TestNormalize:
    def test_lowercase_uppercased(self):
        assert _normalize("dom tubing") == "DOM TUBING"

    def test_mixed_case_uppercased(self):
        assert _normalize("DOM Tubing") == "DOM TUBING"

    def test_already_upper_unchanged(self):
        assert _normalize("HARDWARE") == "HARDWARE"

    def test_part_numbers_uppercased(self):
        assert _normalize("AN6 45 Degree Female Hose End") == "AN6 45 DEGREE FEMALE HOSE END"

    def test_strips_whitespace(self):
        assert _normalize("  Hardware  ") == "HARDWARE"

    def test_empty_string_returns_empty(self):
        assert _normalize("") == ""

    def test_none_returns_none(self):
        assert _normalize(None) is None


class TestBuildItemDoc:
    def test_sets_naming_series(self):
        row = {"item_name": "Hardware", "item_code": "Hardware",
               "item_group": "Part", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": "RPI, INC"}
        doc = build_item_doc(row)
        assert doc["naming_series"] == "STO-ITEM-YYYY."

    def test_stock_item_for_part(self):
        row = {"item_name": "Hardware", "item_code": "Hardware",
               "item_group": "Part", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["is_stock_item"] == 1

    def test_stock_item_for_material(self):
        row = {"item_name": "DOM Tubing", "item_code": "DOM Tubing",
               "item_group": "Material", "stock_uom": "Foot",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["is_stock_item"] == 1

    def test_not_stock_item_for_service(self):
        row = {"item_name": "Labor", "item_code": "Labor",
               "item_group": "Service", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["is_stock_item"] == 0

    def test_item_defaults_company(self):
        row = {"item_name": "Hardware", "item_code": "Hardware",
               "item_group": "Part", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["item_defaults"] == [{"company": "Alumicraft"}]

    def test_supplier_items_populated(self):
        row = {"item_name": "Hardware", "item_code": "Hardware",
               "item_group": "Part", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": "RPI, INC"}
        doc = build_item_doc(row)
        assert doc["supplier_items"] == [{"supplier": "RPI, INC"}]

    def test_empty_supplier_gives_empty_list(self):
        row = {"item_name": "Labor", "item_code": "Labor",
               "item_group": "Service", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["supplier_items"] == []

    def test_normalize_applied_to_item_name(self):
        row = {"item_name": "dom tubing", "item_code": "dom tubing",
               "item_group": "Material", "stock_uom": "Foot",
               "company": "Alumicraft", "supplier": ""}
        doc = build_item_doc(row)
        assert doc["item_name"] == "DOM TUBING"
        assert doc["item_code"] == "DOM TUBING"

    def test_normalize_applied_to_description(self):
        row = {"item_name": "Hardware", "item_code": "Hardware",
               "item_group": "Part", "stock_uom": "Each",
               "company": "Alumicraft", "supplier": "",
               "description": "misc small hardware"}
        doc = build_item_doc(row)
        assert doc["description"] == "MISC SMALL HARDWARE"


class TestValidateMissing:
    def test_all_present_returns_empty(self):
        result = validate_missing({"Each", "Foot"}, {"Each", "Foot", "Sheet"})
        assert result == set()

    def test_missing_items_returned(self):
        result = validate_missing({"Each", "Roll"}, {"Each", "Foot"})
        assert result == {"Roll"}

    def test_empty_required_returns_empty(self):
        result = validate_missing(set(), {"Each", "Foot"})
        assert result == set()
