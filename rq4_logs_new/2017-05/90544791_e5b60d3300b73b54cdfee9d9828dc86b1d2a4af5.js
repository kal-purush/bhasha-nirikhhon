var body;
var lightboxImg;
var lightbox;

window.onload = function () {
  GetImages();
  body = document.getElementsByTagName('body')[0];
  lightbox = document.getElementById('lightbox');
  lightboxImg = document.getElementById('lightboxImg');

  lightbox.style.display = "none";
  lightbox.addEventListener("click", function() {
    ViewLightBox(event);
  });
}

function GetImages() {
  var gallery = document.getElementsByClassName('img');

  for (var n = 0; n < gallery.length; n++) {
    var images = gallery[n].childNodes;

    for (var i = 0; i < images.length; i++) {
      if (images[i].tagName == "DIV") {
        console.log(images[i]);
        images[i].addEventListener("click", function() {
          ViewLightBox(event);
        });
      }
    }
  }
}

function ViewLightBox(event) {

  var img = event.target.parentNode;
  img = img.style.backgroundImage;
  console.log("img " + img);
  img = img.substring(img.indexOf('"') + 1, img.lastIndexOf('"'));

  lightboxImg.setAttribute('src', img);

  var lightboxDisp = (lightbox.style.display == "none");

  lightbox.style.display = (lightboxDisp) ? "flex" : "none";
  body.style.overflowY = (lightboxDisp) ? "hidden" : "scroll";
}