(function( $ ) {
	'use strict';

	$( document ).ready( function() {

		/**
		 * Note: amfpVars variable coming in with all admin PHP vars.
		 */

		// Add new heading and instructions/message.
		$( 'body.auto-menu-active #post-body-content' ).prepend( '<div class="custom-menu-message"></div>' );
		$( 'body.auto-menu-active .custom-menu-message' ).append( '<h3>' + amfpVars.menu_title + '</h3>' );
		$( 'body.auto-menu-active .custom-menu-message' ).append( '<p class="menu-note">' + amfpVars.menu_desc_text + '</p>' );

	});

})( jQuery );