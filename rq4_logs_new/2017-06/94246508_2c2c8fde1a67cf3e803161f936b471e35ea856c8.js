$(document).ready(function(){
"use strict";

	// $('.invisible').css('display', 'none');

	// $('dt').click(function(){
	// 	$('.invisible').toggle();

		$('.button0').click(function(){
			$('dd').toggleClass('invisible');
		});
		
		//bonus
		$('dt').click(function(){
			$(this).css('background-color', 'yellow');
		});

		//button clicked makes last li in each ul have a yellow background	
		$('.button1').click(function(){
			$('ul').each(function(index, element){
			$(element).children().last().css('background-color', 'yellow');
			});
		});

		//when any h3 ic clicked, the li's underneath it should be bolded. 
		$('h3').click(function(){
			$('ul').each(function(index, element){
			$(element).children().css('font-weight', 'bold');
			});
		});

		//when any list item is clicked, first li of the parent ul 
		//should have a font color of blue

		$('li').click(function(){
			$('ul').each(function(index, element){
			$(element).children().first().css('color', 'blue');
			});
		});



	



		// When any list item is clicked, the first li of the parent ul
		// should have a font color of blue. 




		});




