var repeaterFieldsValuesSet = false
jQuery(window).bind('gform_repeater_init_done', function() {
	if(!repeaterFieldsValuesSet) {
			for(var form_id in gfpsrao_settings.row_field_values) {
			var form = jQuery('#gform_' + form_id)
			for(var row in gfpsrao_settings.row_field_values[form_id]) {
				for(var child_id in gfpsrao_settings.row_field_values[form_id][row]) {
					var field_value = gfpsrao_settings.row_field_values[form_id][row][child_id];
					var field_name = 'input_' + child_id + '-1-' + ( parseInt(row) + 1 );
					var field = form.find('[name="' + field_name + '"]');
					if (field.is(':checkbox, :radio')) {
					    form.find('input[name="' + field_name + '"][value="' + field_value + '"]').prop('checked', true);
					} else {
						field.val(field_value);
					}
				}
			}
		}
		repeaterFieldsValuesSet = true;
	}
});