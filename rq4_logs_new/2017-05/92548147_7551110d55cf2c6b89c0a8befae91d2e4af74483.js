function Ground() {
  //POSITION Ground
  this.x=0;
  this.y=550;
  //IMAGE
  this.groundImg = loadImage("http://i.imgur.com/vhFykH0.png");
  this.Render = function() {
    image(this.groundImg, this.x, this.y);
  }
}