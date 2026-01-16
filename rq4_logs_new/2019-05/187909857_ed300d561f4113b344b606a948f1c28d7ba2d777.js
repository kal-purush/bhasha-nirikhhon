function makeList() {
        var data = '["72 percent of Canadians aged 18-79 drink coffee everyday.", "In Italy, the average Barista age is 48, and it is a very respected profession.", "Espresso is regulated by the Italian government because it is considered an essential part of their daily life", "Despite the fact that caffeine is a mild diuretic, you do not lose more fluid than you take in by drinking coffee, so it cannot dehydrate you.", "The Guinness World record holder for the Oldest Cat Ever, a 38-year-old cat named Creme Puff - drank coffee every morning her whole life.", "Back in 1511, leaders in Mecca believed it stimulated radical thinking and banned the drink.", "Legend has it that 9th century goat herders noticed the effect caffeine had on their goats, who appeared to dance after eating coffee berries. A local monk then made a drink with coffee berries and found that it kept him awake at night, thus the original cup of coffee was born.", "The average American spends on average $1,092 a year on coffee. Thats around $20 a week.", "Coffee is the worlds 2nd largest traded commodity.  Crude oil is first."]';
        var array = JSON.parse(data);
        for (var i=0; i<array.length; i++) {
        	var message = document.createElement('li');
        	message.innerHTML = array[i];
        	document.getElementById('messageList').appendChild(message);
        }
    }

// Create a new list item when clicking on the "Add" button
function newElement() {
  var li = document.createElement("li");
  var inputValue = document.getElementById("myInput").value;
  var t = document.createTextNode(inputValue);
  li.appendChild(t);
  if (inputValue === '') {
    alert("You must write something!");
  } else {
    document.getElementById("messageList").appendChild(li);
  }
  document.getElementById("myInput").value = "";

  for (i = 0; i < close.length; i++) {
    close[i].onclick = function() {
      var div = this.parentElement;
      div.style.display = "none";
    }
  }
}

function clearForm() {
    var root = document.getElementById("messageList");
    	while (root.firstChild) {
    		root.removeChild(root.firstChild);
    	}
}

    window.onload = makeList;