$(document).ready(function() {
  var randomNum = Math.floor(Math.random() * 77 + 17);
  $("#randomCount").text(randomNum);

  var num1 = Math.floor(Math.random() * 10 + 1);
  var num2 = Math.floor(Math.random() * 10 + 1);
  var num3 = Math.floor(Math.random() * 10 + 1);
  var num4 = Math.floor(Math.random() * 10 + 1);

  var YourTotal = 0;
  var Yourwins = 0;
  var Yourlosses = 0;

  $("#WinTotal").text(Yourwins);
  $("#LossesTotal").text(Yourlosses);

  function winnah() {
    alert("Winnah!");
    Yourwins++;
    $("#WinTotal").text(Yourwins);
    reset();
  }

  function losah() {
    alert("Losah!");
    Yourlosses++;
    $("#LossesTotal").text(Yourlosses);
    reset();
  }

  $("#blueC").on("click", function() {
    YourTotal = YourTotal + num1;
    console.log("New YourTotal= " + YourTotal);
    $("#Total").text(YourTotal);

    if (YourTotal == randomNum) {
      winnah();
    } else if (YourTotal > randomNum) {
      losah();
    }
  });
  $("#greenC").on("click", function() {
    YourTotal = YourTotal + num2;
    console.log("New YourTotal= " + YourTotal);
    $("#Total").text(YourTotal);
    if (YourTotal == randomNum) {
      winnah();
    } else if (YourTotal > randomNum) {
      losah();
    }
  });
  $("#redC").on("click", function() {
    YourTotal = YourTotal + num3;
    console.log("New YourTotal= " + YourTotal);
    $("#Total").text(YourTotal);
    //sets win/lose conditions
    if (YourTotal == randomNum) {
      winnah();
    } else if (YourTotal > randomNum) {
      losah();
    }
  });

  $("#yellowC").on("click", function() {
    YourTotal = YourTotal + num4;
    console.log("New YourTotal= " + YourTotal);
    $("#Total").text(YourTotal);

    if (YourTotal == randomNum) {
      winnah();
    } else if (YourTotal > randomNum) {
      losah();
    }
  });

  function reset() {
    randomNum = Math.floor(Math.random() * 77 + 17);
    console.log(randomNum);
    $("#randomCount").text(randomNum);
    num1 = Math.floor(Math.random() * 10 + 1);
    num2 = Math.floor(Math.random() * 10 + 1);
    num3 = Math.floor(Math.random() * 10 + 1);
    num4 = Math.floor(Math.random() * 10 + 1);
    YourTotal = 0;
    $("#Total").text(YourTotal);
  }
});