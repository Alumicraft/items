// Intercept doctype loading — every time Item meta is fetched,
// strip mandatory from item_code BEFORE the callback runs.
// This catches Quick Entry which calls with_doctype before rendering.
var _with_doctype = frappe.model.with_doctype;
frappe.model.with_doctype = function (doctype, callback, force) {
    return _with_doctype.call(this, doctype, function () {
        if (doctype === 'Item') {
            var df = frappe.meta.get_docfield('Item', 'item_code');
            if (df) {
                df.reqd = 0;
                df.hidden = 0;
            }
        }
        if (callback) callback();
    }, force);
};

// Custom Quick Entry for Item — live uppercase + auto-fill item_code
frappe.provide('frappe.ui.form');
frappe.ui.form.ItemQuickEntryForm = class ItemQuickEntryForm extends frappe.ui.form.QuickEntryForm {
    render_dialog() {
        super.render_dialog();
        var dialog = this.dialog;

        // Live uppercase on item_name + auto-fill item_code
        dialog.fields_dict.item_name &&
            dialog.fields_dict.item_name.$input.on('change keyup', function () {
                var val = dialog.get_value('item_name');
                if (val) {
                    var upper = val.toUpperCase();
                    dialog.set_value('item_name', upper);
                    var code = dialog.get_value('item_code');
                    if (!code || code === dialog._last_synced_name) {
                        dialog.set_value('item_code', upper);
                    }
                    dialog._last_synced_name = upper;
                }
            });

        // Live uppercase on item_code
        dialog.fields_dict.item_code &&
            dialog.fields_dict.item_code.$input.on('change keyup', function () {
                var val = dialog.get_value('item_code');
                if (val) {
                    dialog.set_value('item_code', val.toUpperCase());
                }
            });
    }
};

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
