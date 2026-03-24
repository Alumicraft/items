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
