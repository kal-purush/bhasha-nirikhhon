var API_KEY = 'dc6zaTOxFJmzC'; //Giphy Public API Key
var URL = 'https://api.giphy.com/v1/gifs/search?api_key='+API_KEY;

var loaded = false;
var formData = {
	q: 'monkeys cats dogs birds',
	limit: 1,
	rating: 'pg',
};
var counter = 0;
var currentSlug = null;

$(document).ready(function(){

	
	$('#checkForRobot').change(function(){
		if($(this).is(':checked')){
		
			if(!loaded){
				$.ajax({
					url: URL,
					data: formData,
					dataType: 'json',
					success: function(data){
						console.log(data, data.data[0].images.downsized.url);
						var dataObj = data.data[0];
						var imgObj = dataObj.images.downsized;
						var imgSrc = imgObj.url;
						var imgWidth = imgObj.width;
						var imgHeight = imgObj.height;

						
						$('<img src="'+imgSrc+'" width="'+imgWidth+'" height="'+imgHeight+'">').appendTo('#giphy_captcha');
						$('.captchaDiv').toggleClass('close');

						currentSlug = dataObj.slug;
					},
					error: function(error){
						console.log(error);
					},
					complete: function(){
						loaded = true;
					}
				});
			}else {
				$('.captchaDiv').toggleClass('close');
			}
			
		}else {
			$('.captchaDiv').toggleClass('close');
		}
	});

	//reload
	$('#reload').click(function(){
		counter++;

		if(loaded){
			
			if(counter > 0){
				formData['offset'] = counter;
			}

			$.ajax({
				url: URL,
				data: formData,
				dataType: 'json',
				beforeSend: function(){
					$('#giphy_captcha').height($('#giphy_captcha img').height());
					$('#giphy_captcha img').remove();
				},
				success: function(data){
					console.log(data, data.data[0].images.downsized.url);
					var dataObj = data.data[0];
					var imgObj = dataObj.images.downsized;
					var imgSrc = imgObj.url;
					var imgWidth = imgObj.width;
					var imgHeight = imgObj.height;

					$('#giphy_captcha').height(imgHeight);
					$('<img src="'+imgSrc+'" width="'+imgWidth+'" height="'+imgHeight+'">').appendTo('#giphy_captcha');

					currentSlug = dataObj.slug;
				},
				error: function(error){
					console.log(error);
				}
			});
		}
		return false;
	});

	//submit function
	$('#submit').click(function(e){

		var btn = $(this),
			spinner = btn.children('.spinner'),
			text = btn.children('.text');


		if($("form")[0].checkValidity()){
			e.preventDefault();

			text.hide(0);
			spinner.fadeIn();
			
			var captcha_field = $('#captcha_input').val();
			var formatedCurrentSlug = currentSlug != null ? currentSlug.replace(/-/g, ' ') : null;
      
      //This is how validation works.
      //However, `slug` doesn't always describe
      //what the GIF is about so 
      //validation is a bit shaky
      //there should be better ideas ;)
			if(formatedCurrentSlug.indexOf(captcha_field) > -1){
				console.log('validated!');
				spinner.fadeOut(300, function(){
					text.html('Succcess!!!').show(0);
					btn.prop('disabled', true);
					$('.captchaDiv').toggleClass('close');
					$('.formData').addClass('disabled');
				});
			}else {
				spinner.fadeOut(300, function(){
					text.html('Try Again :(').show(0);
				});
				
			}
		}
	});
});