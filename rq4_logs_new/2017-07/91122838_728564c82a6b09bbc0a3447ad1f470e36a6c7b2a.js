/**
 * Returns the event target.
 *
 * @since 		1.0.0
 * @param 		object 		event 		The event.
 * @return 		object 		target 		The event target.
 */
function getEventTarget( event ) {

	event.event || window.event;

	return event.target || event.scrElement;

} // getEventTarget()

/**
 * Returns the parent node with the requested class.
 *
 * This is recursive, so it will continue up the DOM tree
 * until the correct parent is found.
 *
 * @since 		1.0.0
 * @param 		object 		el 				The node element.
 * @param 		string 		className 		Name of the class to find.
 * @return 		object 						The parent element.
 */
function getParent( el, className ) {

	var parent = el.parentNode;

	if ( '' !== parent.classList && parent.classList.contains( className ) ) {

		return parent;

	}

	return getParent( parent, className );

} // getParent()

/**
 * Sends data to a PHP handler for saving via AJAX.
 *
 * @since 		1.0.0
 * @param 		array 		paramData 		The data to send to the PHP handler.
 */
var tout = {};

tout.saveAjax = function( paramData ) {
	jQuery.ajax({
		url: ajaxurl,
		type: 'POST',
		async: true,
		cache: false,
		data: paramData,
		success: function ( response ) {
			jQuery( '.button-status' ).html( '<span class="status">' + response  + '</span>' );
			jQuery( '.button-status' ).addClass( 'updated' );
			jQuery( '.button-status' ).fadeIn( 'fast' );
			jQuery( '.button-status' ).fadeOut( 2000 );

			return;
		},
		error: function (xhr, testStatus, error ) {
			jQuery( '.button-status' ).html( '<span class="status">' + error + '</span>' );
			jQuery( '.button-status' ).addClass( 'error' );
			jQuery( '.button-status' ).fadeIn( 'fast' );
			jQuery( '.button-status' ).fadeOut( 2000 );

			return;
		}
	});
};