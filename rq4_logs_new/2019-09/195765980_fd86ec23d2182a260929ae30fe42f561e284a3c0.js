function getResult() {
      var score = $(".starR.on").length;
      //print($(".starR.on").length);
      document.getElementById("score").value = score;
      return score;
}