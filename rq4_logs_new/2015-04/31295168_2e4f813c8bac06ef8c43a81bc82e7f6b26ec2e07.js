(function($){
	"use strict";

	$(document).ready( function() {
		var $format_link   = $('#post-format-link'),
		$format_settings   = $('.et_post_format_setting'),
		$link_settings     = $('.et_link_settings');

		$('.color-picker-hex').wpColorPicker();

		if ( $format_link.is(':checked') )
			$link_settings.show();

		$('.post-format').change( function() {
			var $this = $(this);

			$format_settings.hide();

			if ( $this.is( '#post-format-link' ) )
				$link_settings.show();
		} );
	} );
})(jQuery)