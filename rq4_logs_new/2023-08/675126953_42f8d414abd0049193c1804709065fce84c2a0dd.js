var countDownDate = new Date("Nov 25, 2022 12:00:00").getTime();

var x = setInterval(function() {

  var now = new Date().getTime();

  var distance = countDownDate - now;

  var days = Math.floor(distance / (1000 * 60 * 60 * 24));
  var hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
  var minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
  var seconds = Math.floor((distance % (1000 * 60)) / 1000);


  document.getElementById("demo").innerHTML = days + "d " + hours + "h "
  + minutes + "m " + seconds + "s"+"<br>"+new Date(countDownDate).toLocaleDateString();

  if (distance < 0) {
    clearInterval(x);
    document.getElementById("demo").innerHTML = "Now! Check information <a href='support.html'target='_blank' class='text-link'>here</a><span class='point'>.</span>";
  }
}, 1000);