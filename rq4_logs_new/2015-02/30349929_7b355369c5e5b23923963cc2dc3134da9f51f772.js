$.fn.existsChecker = function(){
	return this.each(function(){
		var interval;

		$(this).on('keyup', function(){
			var self = $(this),
			selfType = self.data('type'),
			selfValue,
			feedback = $('.check-exists-feedback[data-type=' + selfType + ']');

			if(interval === undefined){
				interval = setInterval(function(){
					if(selfValue !== self.val()){
						selfValue = self.val();

						if(selfValue.length > 2){
							$.ajax({
								url: 'check.php',
								type: 'get',
								dataType: 'json',
								data: {
									type: selfType,
									value: selfValue
								},
								success: function(data){
									if(data.exists !== undefined){
										if(data.exists === true){
											feedback.text('Already taken.');
										}else{
											feedback.text('That is available!');
										}
									}
								},
								error: function(){
									// Something went wrong
								}
							});
						}
					}
				}, 2000);
			}
		});
	});
};