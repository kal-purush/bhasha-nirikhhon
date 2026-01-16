(function($){
	chrome.storage.local.get('userInfo', function(data) {
		var token=data.userInfo.token;
		var email=data.userInfo.email;
		$.authenticate={
			post: function(url,options,callback){
				options.email=email;
				options.token=token;
				$.post(url,options,
					callback(data,status);
				)
			}
		}
	})
)(jQuery);