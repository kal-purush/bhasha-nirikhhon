$(document).ready(function () {
	
	//This function creates the divs and sets their CSS traits.
	function divPrep(cSize, divNum){
		var divSize = cSize / divNum;
		var divSizepx = divSize + 'px';
		var squares = divNum * divNum;
		for (var x = 1; x <= squares; x++) {
        $('#container').append('<div></div>');
		}
		$(divs).css({
			'height': divSizepx,
			'width': divSizepx,
			'border-radius': divSizepx,
			'background-color':'white'
		})
	}
	
	//This function takes the current color of a div, and return a darker color.
	function makeDarker(){
		var rgb = $(this).css('background-color')
		rgb = rgb.replace(/[^\d,]/g, '').split(',');
		var newrgb = 'rgb(' + Math.max(0,rgb[0]-20) + ',' + Math.max(0,rgb[1]-20) + ',' + Math.max(0,rgb[2]-20) + ')'
		return newrgb
	}
	
	//This function generates a random RGB color.
	function randomColor(){
		var r = Math.floor((Math.random()) * 256);
        var g = Math.floor((Math.random()) * 256);
        var b = Math.floor((Math.random()) * 256);
        var color = 'rgb(' + r + ',' + g + ',' + b + ')';
		return color;
	}
	
	//This function assigns a background color to a div if it is currently white.
	function sketch() {
        if ($(this).css("background-color") === 'rgb(255, 255, 255)'){
            $(this).css("background-color", randomColor);
        }
		else if ($(this).css("background-color") != 'rgb(255, 255, 255)') {
			$(this).css("background-color", makeDarker);
		}
	}

	//This is where the container box is defined.
    var divs = '#container div';
    var cSize = 960;
    var cSizepx = cSize + 'px';
    var divNum = 16;
    $('#container').css({
        'height': cSizepx,
        'width': cSizepx,
        'border': "1px solid black"
    })
    
	divPrep (cSize, divNum)
    $(divs).mouseover(sketch)

    //Document after button press
    $(document).on('click', 'button', function () {
        $(divs).remove();
		var check = 0;
		while (check < 1){
			var divNum = prompt("How many circles across would you like from 1 to 96?");
			if (divNum <= 96){check = 1}
		}
		check = 0
		divPrep (cSize, divNum)
		$(divs).mouseover(sketch)
    })

	setInterval(function(){
		$('.header h1').css('color',randomColor)
		$('.header p').css('color',randomColor)
		$('.header').css('background-color',randomColor)
		$('#container').css('border-color',randomColor)
	}, 1000)


});