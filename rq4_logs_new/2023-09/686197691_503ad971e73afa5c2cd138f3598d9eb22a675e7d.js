function topnavbutton() {
    var x = document.getElementById("myTopnav");
    if (x.className === "topnav") {
        x.className += " responsive";
    } else {
        x.className = "topnav";
    }
    var y = document.getElementById("myColumn");
    if (y.className === "column left") {
        y.className += " responsive";
    } else {
        y.className = "column left";
    }
}