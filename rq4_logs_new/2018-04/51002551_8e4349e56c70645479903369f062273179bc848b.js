/**
 * The analytics file for Drift related actions
 * @author Jack
 * @see https://help.drift.com/developer-docs/widget-events-to-send-to-things-like-google-analytics
 * @see https://help.drift.com/developer-docs/widget-api
 */
drift.on('ready', function(api, payload) {
	// interact with the api here
	window.drift.on('startConversation', function() {
		ga('send', 'event', 'Drift Widget', 'Chat Started');
	});
	window.drift.on("emailCapture", function(data) {
		ga('send', 'event', 'Drift Widget', 'Email Captured');
	});
	window.drift.on("scheduling:meetingBooked", function(data) {
		ga('send', 'event', 'Drift Widget', 'Meeting Booked');
	});
	// show the widget when you receive a message
	window.drift.on('message', function(e) {
		if (!e.data.sidebarOpen) {
			api.widget.show()
		}
	})
})