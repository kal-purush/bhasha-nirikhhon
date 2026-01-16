var c = document.getElementById("hangmana");
var context = c.getContext("2d");
context.strokeStyle = "#FF6223";
context.lineWidth = 3;

var draw = function(x1, y1, x2, y2) {
    context.beginPath();
    context.moveTo(x1, y1);
    context.lineTo(x2, y2);
    context.stroke();}
var initialDrawing = function() {
    draw(20, 180, 280, 180);
    draw(40, 180, 40, 20);
    draw(20, 30, 130, 30);};
function body() {
    draw(120, 70, 120, 110);};
function head() {
    context.beginPath();
    context.arc(120, 60, 10, 0, 2 * Math.PI);
    context.stroke();};
function arms() {
    draw(120, 90, 105, 75);
    draw(120, 90, 135, 75);};
function legs() {
    draw(120, 110, 105, 130);
    draw(120, 110, 135, 130);};

initialDrawing();
var drawThings = [rope, head, body, arms, legs];

var categories = [
    ["SPIDERMAN", "CONJURING", "EXORCIST", "TWILIGHT", "GLADIATOR"],
    ["BAHAMAS", "AUSTRIA", "SYDNEY", "ETHIOPIA", "BRZIL"],
    ["MONKEY", "GORILLA", "DEER", "ZEBRA", "IGUANA"],
    ["FOOTBALL", "SKATING", "RUGBY", "BASKETBALL", "HOCKEY"]
];
var index = 0,
    word, TheWinner, N, first, second, index1 = 0;
var voc = "AEIOU";

function changeCategory() {

    var select = document.getElementById("select");
    var selectedValue = select.options[select.selectedIndex].value;
    index = Number(selectedValue);
    index1 = 0;
    reset();
};

function rope() {
    draw(120, 25, 120, 50);};

function reset() {
    $("li").removeClass("disable");
    document.getElementById("keyword").innerHTML = "";
    context.clearRect(0, 0, 300, 200);
    initialDrawing();
    play();};
function vowels() {
    N = "";
    for (i in word) {
        if (voc.indexOf(word.charAt(i)) != -1) {
            N += word.charAt(i) + " ";
            TheWinner--;
            var vowel = document.getElementById(word.charAt(i));
        } else
            N += "_ ";
                }
    document.getElementById("keyword").innerHTML = N;
}
function play() {
    word = categories[index][Math.floor(Math.random() * 5)];
    TheWinner = word.length;
    vowels();
};
play();
index = 0;
$('li').click(function() { // check
    if (this.className != "disable") {
        var letter = this.innerHTML;
        if (word.indexOf(letter) != -1) {
            for (i in word)
                if (letter == word.charAt(i)) {
                    if (i == 0) {
                        first = letter;
                        var res = N.substring(1, N.length);
                        first += res;
                    } else {
                        first = N.substring(0, 2 * i);
                        second = N.substring(2 * i + 1, N.length);
                        first += letter + second;
                    }
                    N = first;
                    TheWinner--;
                    document.getElementById("keyword").innerHTML = N;
                }
        } else {
            drawThings[index1]();
            index1++;
        }
        $(this).addClass("disable");
        if (index1 == 5) {
            document.getElementById("keyword").innerHTML = "Ooops!!You lost.";
            $("li").addClass("disable");
            index1 = 0;
        }
        if (TheWinner == 0) {
            document.getElementById("keyword").innerHTML = "Congrats!!!You conquered it.";
            $("li").addClass("disable");
            index1 = 0;
        }

        else{
        	 document.getElementById("keyword").innerHTML = "Ooops!!You lost.";
        }
    }
});