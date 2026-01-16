$(document).ready(function() {

	
	$('button').click(function () { //added jquery click() method passing an anonymous 
		//callback function for the click event
		$('button').removeClass('selected'); // removes class from all buttons(must go before addClass() line)
		$(this).addClass('selected');//'this' refers to a single element/button responding to 
		//clk event, in this case button
		
		//added ?jsoncallback=? to query string below to convert xml into json at flickr feed
		var flickerAPI = 'https://api.flickr.com/services/feeds/photos_public.gne?jsoncallback=?';
		var animal = $(this).text(); //gets all text inside (the clicked button) element
		var flickrOptions = {
			tags: animal,   //see flickr api, sends photo tag text from each button when clkd
			format: 'json'  //normally its xml, must tell flickr need json
		};
		//ajax call using $.getJSON method, 3 args, 1-url to resource, 2-data we send with url
		// 3-callback program that runs when response is received
		$.getJSON(flickerAPI, flickrOptions, function(response) {
			 // console.log(typeof response); //prints 'object' in console
			var photoHTML = '<ul>';
			//$.each() method jQuery loop, 2 args, array and callback
			//variable 'response' was ging to data received from flickr, items is the name of the array from flickr
			//response.items holds entire array of photos
			$.each(response.items, function(i, photo) { //gave $.each callback func a variable name 'photo' 
				//for 2nd arg representing a different value for each array property, 
				//and i variable 1st arg represents array index which changes as loops
				photoHTML += '<li class = "grid-25 tablet-grid-50">';
				photoHTML += '<a class="image" href="' + photo.link + '" >'; //link is property name from flickr
				photoHTML += '<img src="' + photo.media.m + '">'; //media.m is property name from flickr api,has 
				//to be a string inside of a string
				photoHTML += '</a>';
				photoHTML += '</li>';
			}); //end each
			photoHTML += '</ul>';
			$('#photos').html(photoHTML);
		}); //end getJSON
	}); //end button	
}); //end ready