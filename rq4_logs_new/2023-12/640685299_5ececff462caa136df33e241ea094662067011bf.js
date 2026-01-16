function changeFirstY1() {
    var image = document.getElementById('1img');
    if (image.src.match("1.jpg")) {
        image.src = "2.jpg";
    } else {
        image.src = "1.jpg";
    }
}

function changeFirstY2() {
    var image = document.getElementById('2img');
    if (image.src.match("3.jpg")) {
        image.src = "4.jpg";
    } else {
        image.src = "3.jpg";
    }
}

function changeFirstY3() {
    var image = document.getElementById('3img');
    if (image.src.match("5.jpg")) {
        image.src = "6.jpg";
    } else {
        image.src = "5.jpg";
    }
}

function changeSecondY1() {
    var image = document.getElementById('4img');
    if (image.src.match("7.jpg")) {
        image.src = "8.jpg";
    } else {
        image.src = "7.jpg";
    }
}

function changeSecondY2() {
    var image = document.getElementById('5img');
    if (image.src.match("9.jpg")) {
        image.src = "10.jpg";
    } else {
        image.src = "9.jpg";
    }
}

function changeSecondY3() {
    var image = document.getElementById('6img');
    if (image.src.match("11.jpg")) {
        image.src = "12.jpg";
    } else {
        image.src = "11.jpg";
    }
}

function changeSecondY4() {
    var image = document.getElementById('7img');
    if (image.src.match("13.jpg")) {
        image.src = "14.jpg";
    } else {
        image.src = "13.jpg";
    }
}
function changeSecondY5() {
    var image = document.getElementById('8img');
    if (image.src.match("15.jpg")) {
        image.src = "16.jpg";
    } else {
        image.src = "15.jpg";
    }
}
function changeColor(obj) {
   obj.style.color ="purple";
    obj.style.backgroundColor ="#e5d5ec";
}

function mOver(obj) {
  obj.innerHTML = "Example"
}

function mOut(obj) {
  obj.innerHTML = "New Example"
}