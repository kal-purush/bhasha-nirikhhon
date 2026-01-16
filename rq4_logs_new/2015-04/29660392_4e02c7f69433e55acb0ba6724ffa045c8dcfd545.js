var autoclose;

(function($) {
    "use strict";
    autoclose = php_vars.close_seconds*1000;
	setTimeout(
		function(){
			$(".wpsh_fixed").fadeOut("slow");
		}, autoclose);
}
(jQuery));