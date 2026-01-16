//write first method
function sleepIn(x,y) {
    return x;
}

//write second method
function nextOne(param1, param2) {
    return param1;
}

//function runs on click and outputs test cases you create to page
function tester() {
    document.getElementById("output").innerHTML += sleepIn(true, false);
    document.getElementById("output").innerHTML += nextOne(true, false);
    //test third method, etc
}

//JavaScript Problem #1 Sleep In
function sleep_in(x, y) {
    var sleep_in = !x || y;
    return sleep_in;
}

//JavaScript Problem #2 Monkey Trouble
function monkey_trouble(a, b) {
    var a_smile = a && a;
    var b_smile = b && b;
    var trouble = (a_smile && b_smile) || (!a_smile && !b_smile);
    return trouble;
}

//JavaScript Problem #3 String Times
function string_times(m, n) {
    var name = m;
    if (m > 0) {
        for(var n; n - 1 > 0; n--) {
            name = name + m;
            }
    } else {
            name = "";
        }
    return name;
}



//JavaScript Problem #4 Front Times
//function front_times(m, n)
