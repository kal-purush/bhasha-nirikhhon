var picture= document.getElementById('pic'),
    leftPosition = 0,
    goRight = true;
     picture.style.left = 'absolute';
     
function setPosition (left) {
 leftPosition += left;
 picture.style.left = leftPosition + 'px';
}
function animate () {
 if (leftPosition > 1100) {
   goRight = false;
 }
 if (leftPosition < 0) {
   goRight = true;
   }
   return goRight ? setPosition(2) : setPosition(-2);
 }
setInterval(animate, 15);