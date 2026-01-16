var numbers = document.getElementsByClassName('digit');
var clicks = 0;
var h = new Object();
h[1] = "url(/images/one.png)";
h[2] = "url(/images/two.png)";
h[3] = "url(/images/three.png)";
h[4] = "url(/images/four.png)";
h[5] = "url(/images/five.png)";
h[6] = "url(/images/six.png)";

  var oneOf = document.getElementsByClassName('oneOf');
  for (var i=0; i<6; i++){
    numbers[i].addEventListener("click",function(){      
        if (clicks < 4){
          if (this.innerHTML == '1'){
            oneOf[clicks].style.backgroundImage = h[1];
            document.getElementById('secretCode').value += '1';
          }
          if (this.innerHTML == '2'){
            oneOf[clicks].style.backgroundImage = h[2];
            document.getElementById('secretCode').value += '2';
          }
          if (this.innerHTML == '3'){
            oneOf[clicks].style.backgroundImage = h[3];
            document.getElementById('secretCode').value += '3';
          }
          if (this.innerHTML == '4'){
            oneOf[clicks].style.backgroundImage = h[4];
            document.getElementById('secretCode').value += '4';
          }
          if (this.innerHTML == '5'){
            oneOf[clicks].style.backgroundImage = h[5];
            document.getElementById('secretCode').value += '5';
          }
          if (this.innerHTML == '6'){
            oneOf[clicks].style.backgroundImage = h[6];
            document.getElementById('secretCode').value += '6';
          }
          clicks = clicks + 1;
      }
    });
  }