let myImage = document.querySelector('img');

myImage.onclick = function() {
  let mySrc = myImage.getAttribute('src');
  if(mySrc === 'images/nomalft.png') {
    myImage.setAttribute ('src','images/nomalresult.png');
  } else {
    myImage.setAttribute ('src','images/nomalft.png');
  }
}