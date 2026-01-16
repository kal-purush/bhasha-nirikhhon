(function($) {
	function hide_error() {
		$('#springnet-error-console').hide();
	}
	function display_error(msg) {
		$('#springnet-error-console').text(msg);
		$('#springnet-error-console').show();
	}
	
	function loading_show() {
		$('#springnet-loading-icon').show();
	}
	function loading_hide() {
		$('#springnet-loading-icon').hide();
	}
	$(document).ready(function(){
		
		$('#edit-springnet-geonet-lookup').click(function(){
			hide_error();
			loading_show();
			geonet = $('#edit-springnet-geonet-name').val();
			$.getJSON("/admin/config/services/springnet/georesolve/"+geonet, function(res) {
				loading_hide();
				if(res.hostname == "invalid") {
					display_error("GeoNetwork cannot be resolved; does not exist.")
					$('#edit-springnet-geonet-hostname').val('');
					$('#edit-springnet-geonet-address').val('');
					$('#edit-springnet-geonet-resource').val('');
				} else {
					$('#edit-springnet-geonet-hostname').val(res.hostname);				
					$('#edit-springnet-geonet-address').val(res.address);
					$('#edit-springnet-geonet-resource').val(res.resource);
				}
			});
			return false;
		});
	});
})(jQuery);