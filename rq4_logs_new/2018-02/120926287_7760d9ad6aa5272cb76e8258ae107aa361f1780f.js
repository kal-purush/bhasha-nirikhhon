class TextureLoader {
  constructor() {
    this.textures = new Array();
  }

  getTexture(texture) {
    if(texture < 0) {
      return false;
    }
    if(!this.textures[texture]) {
      var tex = new Image();
      tex.src = "textures/"+texture+".png";
      this.textures[texture] = tex;
    }
    return this.textures[texture];
  }
}