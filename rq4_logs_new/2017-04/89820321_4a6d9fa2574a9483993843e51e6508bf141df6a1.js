function simpleslider(container, imagelist, auto) {
  
  this.index = 0;
  this.imagelist = imagelist;
  this.container = container;
  
  this.setsrc();
  
  if ( auto ) {
    var that = this;
    setInterval(function() {
      that.prev();
    }, 2000);
  }
  
}

simpleslider.prototype.setsrc = function() {
  var slide = this.container.getElementsByClassName("slide")[0]; 
  slide.src = this.imagelist[this.index];
}

simpleslider.prototype.next = function() {
  this.index = (this.index+1) % this.imagelist.length;
  this.setsrc();
}

simpleslider.prototype.prev = function() {
  this.index = (this.index-1) == -1 ? this.imagelist.length-1 : this.index-1;
  this.setsrc();
}

var imageList = [
 "https://source.unsplash.com/random/200x150",
 "https://source.unsplash.com/random/100x100",
 "https://source.unsplash.com/random/200x200",
 "https://source.unsplash.com/random/500x500"];
var container = document.getElementsByClassName("slider-container")[0];

var slider = new simpleslider(container, imageList, false);

var next = document.getElementById('next');
var prev = document.getElementById('prev');

next.onclick = function() {
  slider.next();
};
prev.onclick = function() {
    slider.prev();
};
