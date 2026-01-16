$(document).ready(function(){

	'use strict';

	var $accordion = $('#accordion'),
		$accordionTitle1 = $('#accordion-title-1'),
		$accordionTitle2 = $('#accordion-title-2'),
		$accordionTitle3 = $('#accordion-title-3'),
		$accordionTitle4 = $('#accordion-title-4'),
		$accordionLinks = $('#accordion').children('h2');

	$accordionTitle1.on('click', function() {

		var $collapse1 = $('#collapse-1');

		if ($collapse1.hasClass('hidden')) {

			$accordion.children('div').addClass('hidden');

			$collapse1.removeClass('hidden');

		} else {

			$collapse1.addClass('hidden');

		}

	});

	$accordionTitle2.on('click', function() {

		var $collapse2 = $('#collapse-2');

		if ($collapse2.hasClass('hidden')) {

			$accordion.children('div').addClass('hidden');

			$collapse2.removeClass('hidden');

		} else {

			$collapse2.addClass('hidden');

		}

	});

	$accordionTitle3.on('click', function() {

		var $collapse3 = $('#collapse-3');

		if ($collapse3.hasClass('hidden')) {

			$accordion.children('div').addClass('hidden');

			$collapse3.removeClass('hidden');

		} else {

			$collapse3.addClass('hidden');

		}

	});

	$accordionTitle4.on('click', function() {

		var $collapse4 = $('#collapse-4');

		if ($collapse4.hasClass('hidden')) {

			$accordion.children('div').addClass('hidden');

			$collapse4.removeClass('hidden');

		} else {

			$collapse4.addClass('hidden');

		}

	});

	// Create on click functions for all accordion titles

	// Begin Shopping List Stuff
	var $totalPrice = $('#total-price'),
		shoppingList = [],
		total = 0.00;

	// Make total display initially
	$totalPrice.html('Your total grocery bill is: $' + total.toFixed(2));

	$('#add-groceries').on('click', function(){

		addGroceries();

	});

	// Target the form to add items, price, and their quantity
	function addGroceries(){

		var $growingFoodList = $('#growing-food-list'),
			$growingPriceList = $('#growing-price-list'),
			$totalPrice = $('#total-price'),
			newItem = $('#food-to-add').val(),
			newPrice = $('#price-to-add').val(),
			newQuantity = $('#quantity-to-add').val();

		// Validate that newItem is not null
		if (newItem === '') {

			alert('Please enter a name for the item you are buying');

		}

		// Validate that newPrice is not null
		if (newPrice === '') {

			alert('Please enter a price for the item you are buying');

		}

		if (newPrice < 0) {

			alert('The price must be positive');

		} else if ($.isNumeric(newPrice)) {

			// Add <li> to #growing-food-list
			$growingFoodList.append('<li>' + newItem + ' is</li>');

			// Add <li> to #growingPriceList
			$growingPriceList.append('<li>' + '$' + newPrice + '</li>');

			// Continuously update the total everytime a new item is added
			total += parseFloat(newPrice);
			$totalPrice.html('Total price: $' + total.toFixed(2));

		} else {

			alert('Please enter a valid number');

		}

	}

});