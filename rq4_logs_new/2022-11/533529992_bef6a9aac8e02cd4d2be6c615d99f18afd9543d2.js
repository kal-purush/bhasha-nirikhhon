/*////////////////////////////////////////////////////////////////////*/  
/* overlay effect */

function on() {
    document.getElementById("overlay").style.display = "block";
  }
  
  function off() {
    document.getElementById("overlay").style.display = "none";
  }
  

/*////////////////////////////////////////////////////////////////////*/  
/* on fullscreen style */

  var elem = document.documentElement;

  function openFullscreen() {
    if (elem.requestFullscreen) { //exitFullscreen
        elem.requestFullscreen();
      } else if (elem.webkitRequestFullscreen) { /* Safari */
        elem.webkitRequestFullscreen();
      } else if (elem.msRequestFullscreen) { /* IE11 */
        elem.msRequestFullscreen();
      }
    }


/*////////////////////////////////////////////////////////////////////*/



//moving the absolute position with js
//CSS #188 html #101 Js #33 

var left = 0;

  function frame() {
    var element = document.querySelector('.testdiv_1710');
    left += 2;
    element.style.left = left + 'px';
    if (left >= 300) {
      clearInterval(id);
    }
  }

  var id = setInterval(frame, 10);

  frame();