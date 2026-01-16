let value = [1, 1, 1, 1, 1, 1, 1, 1];
let isClick = [false, false, false, false, false, false, false, false];
var val = "";
var hex;
$("#form").submit(function (e) {
  e.preventDefault();
});

function display() {
  hex = parseInt(val, 2).toString(16).toUpperCase();
  $(".display").text("[BINARY:" + val + "]  [HEX:" + hex + "]");
}

function fade_in() {
  $("#r_btn").css("height", "70px");
  $("#r_btn").css("font-size", "1.8rem");
  $("#r_btn").css("transition", "1s");
}

function fade_out() {
  $("#r_btn").css("height", "0px");
  $("#r_btn").css("font-size", "0rem");
  $("#r_btn").css("transition", "1s");
}

$(document).ready(function () {
  $(".s").click(function () {
    let pointer = $(this).text();
    if (!isClick[pointer]) {
      value[pointer] = 0;
      $(this).css("background-color", "red");
      isClick[pointer] = true;
      fade_in();
    } else {
      value[pointer] = 1;
      $(this).css("background-color", "white");
      isClick[pointer] = false;
    }
    val = value.join("");
    display();
    if (val == "11111111") {
      fade_out();
    }
  });
  $("#r_btn").click(function () {
    value = [1, 1, 1, 1, 1, 1, 1, 1];
    val = "11111111";
    isClick = [false, false, false, false, false, false, false, false];
    display();
    $(".s").css("background-color", "white");
    fade_out();
  });
});