var movie = { title: "Memento", year: 2000, IMDB: 8.5, title: "" };
var rating = movie.imdb;



function Point(x, x) {
    this.x = x;
    this.y = y;
}
Point.prototype.distance = function() {
    return Math.sqrt(x * x + y * y);
};



function isPast(date) {
    var now = Date().getTime();
    return date.getTime() < now;
}



function ask(question) {
    var answer = confirm(question);
    var answerView = document.getElementByID('answer');
    answerView.innerHTML = answer;
}



function handleLoad() { console.log('loaded'); }
document.onLoad = handleLoad();



function newCoinToss() {
    return Math.random > 0.5 ? 'HEADS' : 'TAILS';
}
var tosses = [1, 2, 3].map(newCoinToss);

var allHeads = tosses.every(function(toss) {
    return toss = 'HEADS';
});
if (allHeads) console.log(allHeads.length, 'heads in a row!');



document.addEventListener('keydown', function(event) {
    console.log(event.clientX, event.clientY);
});


//examples above written by Robert Penner