var count = 0;
var review = 0;

function evenNumber() {
	document.getElementsByClassName('circle')[0].style.backgroundColor = "white";
	review = review - 1;
	document.getElementById('review_counter').innerHTML = review;

}

function oddNumber() {
	document.getElementsByClassName('circle')[0].style.backgroundColor = "#589442";
	review = review + 1;
	document.getElementById('review_counter').innerHTML = review;
}


document.getElementsByClassName('circle')[0].addEventListener('click', function() {
	if (count % 2 === 0) {
        oddNumber();
    }
    else {
        evenNumber();
    }

    count++;
});