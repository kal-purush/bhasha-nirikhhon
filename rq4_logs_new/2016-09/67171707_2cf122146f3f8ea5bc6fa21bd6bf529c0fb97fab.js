$(document).ready(function() {

var shoppingLists = {}; //can hold multiple shopping lists as key/value pairs 
var sessionList = []; //the shopping list to be used for current session. 
var listName = ''; //name of the list to be displayed. 

function storeItem(itemName) {
	sessionList.push(itemName);
	localStorage.setItem("shoppingLists", JSON.stringify(shoppingLists));
}

function addItem(itemName, isNewItem) {
	var list = $('#shopping-list');
	$(list).append('<li>' + itemName + '</li>');
	$(list).find('li:last-child').prepend('<span class="glyphicon glyphicon-ok-sign"></span>');
	$(list).find('li:last-child').append('<span class="glyphicon glyphicon-remove-circle"></span>');
	$('#new-item-name').val('');
	//update the saved list if a new item has been added
	if (isNewItem) {
		storeItem(itemName);
	}
}



function removeItem(itemName) {
	var index = sessionList.indexOf(itemName);
	//remove the deleted item from the session list array
	sessionList.splice(index, 1);
	//update local storage
	localStorage.setItem("shoppingLists", JSON.stringify(shoppingLists));
}


//Add Item when form submitted
$('form').submit(function(ev) {
	ev.preventDefault();
	addItem($('#new-item-name').val(), true);

});

//Add Item when add button clicked
$('#add-link').click(function(ev) {
	// e.preventDefault();
	addItem($('#new-item-name').val(), true);
	$('#new-item-name').focus();

});

//Handle Shopping List Item Events
$('#shopping-list').click(function(ev) {
	var target = $(ev.target);
	var item = target.closest('li');
	//mark item completed and move to completed-list
	if (target.hasClass('glyphicon-ok-sign')) {
		$(item).removeClass('appear');
		$(item).addClass('vanish');
		setTimeout(function() {
			$(item).appendTo('#completed-list');
			$(item).children('span').removeClass('glyphicon-ok-sign');
			$(item).children('span').addClass('glyphicon-ok');
			$(item).children('span').first().append('<span class="glyphicon glyphicon-refresh"></span>');
			$(item).css({textDecoration: 'line-through'});
			$(item).addClass('appear');
			$(item).removeClass('vanish');
			$(item).append('<span class="glyphicon glyphicon-remove-circle"></span>');
		}, 250);
	}
	//delete an item completely
	if (target.hasClass('glyphicon-remove-circle')) {
		$(item).removeClass('appear');
		$(item).addClass('vanish');
		removeItem($(item).text());
		setTimeout(function() {
			$(item).remove();
		}, 250);
	}


});


//sort alphabetically
$('#sort').click(function() {
	var listText = []
	$('#shopping-list').children('li').each(function() {
		listText.push($(this).text());
	});
	var i = 0
	listText.sort();
	$('#shopping-list').children('li').contents().filter(function() {
		if (this.nodeType === 3){
			this.nodeValue = listText[i];
			i++;
		}
	});
});

//Handle Completed-List Item Events
$('#completed-list').click(function(ev) {
	var target = $(ev.target);
	var item = target.closest('li');
	//delete an item completely
	if (target.hasClass('glyphicon-remove-circle')) {
		$(item).removeClass('appear');
		$(item).addClass('vanish');
		removeItem($(item).text());
		setTimeout(function() {
			$(item).remove();
		}, 250);
	}
	//restore an item to the main shopping list
	if (target.hasClass('glyphicon-refresh')) {
		$(item).removeClass('appear');
		$(item).addClass('vanish');
		setTimeout(function() {
			$(item).appendTo('#shopping-list');
			target.remove();
			$(item).children('span').removeClass('glyphicon-ok');
			$(item).children('span').first().addClass('glyphicon-ok-sign');
			$(item).removeClass('vanish');
			$(item).css({textDecoration:'none'});
			$(item).addClass('appear');
		}, 250);
	}
});


//User selects which list to display from nav menu
//or reset the list to default values 
$('#list-chooser').click(function(ev) {
	var selected = $(ev.target).attr('id');
	$(selected).addClass('disabled');
	if (selected === "moms-list") {
		listName = 'MomsList';
		$('#evans-list').removeClass('disabled');
		$('#shopping-list').children('li').remove();
		$('#completed-list').children('li').remove();
		$('#list-name-heading').text('Mom\'s Shopping List');
		loadData('MomsList');
	} else if (selected === 'evans-list') {
		listName = 'EvansList';
		$('#moms-list').removeClass('disabled');
		$('#shopping-list').children('li').remove();
		$('#completed-list').children('li').remove();
		$('#list-name-heading').text('Evan\'s Shopping List');
		loadData('EvansList');
	} else if (selected === 'reset-list') {
		$('#shopping-list').children('li').remove();
		$('#completed-list').children('li').remove();
		localStorage.clear('shoppingLists');
		initializeData(listName);

	}

});


//A default shopping list. Each user can have a unique list, with
//each list as a key/value pair object, where the key is the list name or user name and the value is an array of items in the list. 
var myList =	{
					"MomsList"	: 	[
										"milk", "eggs", "sugar", "flour", "peanut butter", "cereal", "potatoes", "bread", "tomatoes", "lettuce", "butter"
									],
					"EvansList"	: 	[
										"ice cream", "pizza", "hamburger", "buns", "Mtn. Dew", "cheese", "chips", "salsa"
									]
				}



function isListStored() {
	var storeCheck = JSON.parse(localStorage.getItem("shoppingLists"));
	if (!storeCheck) { return false}
}

//run this first on page load
//test if a list has previously been saved, then load and use it, or if not then
// save the default list and begin using it.
if (isListStored()) { 
	loadData('MomsList');
} else {
	initializeData('MomsList');
}

//loads the existing lists from local storage
function loadData(listName) {
		//load the shopping lists collection from local storage
		shoppingLists = JSON.parse(localStorage.getItem("shoppingLists"));
		//set the current working list 
		sessionList = shoppingLists[listName];

		//call the function to display the list on screen, but set save to false since local storage is up to date


		sessionList.forEach(function(item) {
			addItem(item, false);
		});
}

//creates a default list and initializes a space for it in local storage
function initializeData(listName) {
		//use a list with default values
		shoppingLists = myList;
		sessionList = shoppingLists[listName];
		//create the initial list collection in local storage
		localStorage.setItem("shoppingLists", JSON.stringify(shoppingLists));
		//call the function to display the list on screen, but set save to false since local storage is up to date
		sessionList.forEach(function(item) {
			addItem(item, false);
		});
}

});

