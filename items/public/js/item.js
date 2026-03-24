$(document).on('app_ready', function () {
    // Make item_code not mandatory in meta — fixes Quick Entry validation
    frappe.model.with_doctype('Item', function () {
        var df = frappe.meta.get_docfield('Item', 'item_code');
        if (df) {
            df.reqd = 0;
            df.hidden = 0;
        }
    });
});

frappe.ui.form.on('Item', {
    refresh(frm) {
        frm.set_df_property('item_code', 'hidden', 0);
        frm.set_df_property('item_code', 'read_only', 0);
        frm.set_df_property('item_code', 'reqd', 0);
    },

    item_code(frm) {
        if (frm.doc.item_code) {
            frm.set_value('item_code', frm.doc.item_code.toUpperCase());
        }
    },

    item_name(frm) {
        if (frm.doc.item_name) {
            frm.set_value('item_name', frm.doc.item_name.toUpperCase());
            if (!frm.doc.item_code || frm.doc.item_code === frm.doc.__last_synced_name) {
                frm.set_value('item_code', frm.doc.item_name.toUpperCase());
            }
            frm.doc.__last_synced_name = frm.doc.item_name.toUpperCase();
        }
    },

    description(frm) {
        if (frm.doc.description) {
            frm.set_value('description', frm.doc.description.toUpperCase());
        }
    }
});
