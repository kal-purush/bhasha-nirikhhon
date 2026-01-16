class Alien {
  constructor(x, y, w, h) {
    this.x = x;
    this.y = y;
    this.w = w;
    this.h = h;
    this.speed = 0.5;
  }

  show() {
    if (step == 1) {
      image(alienImg1, this.x, this.y, this.w, this.h);
    } else if (step == -1) {
      image(alienImg2, this.x, this.y, this.w, this.h);
    }
  }

  move() {
    if (direction == 1) {
      this.x += this.speed;
    } else if (direction == -1) {
      this.x -= this.speed;
    }

    if ((this.x - (this.w / 2)) <= 5 || (this.x + (this.w / 2)) >= width - 5) {
      direction *= -1;
      for (let a of aliens) {
        a.y += 7;
      }
    }
  }

  breach() {
    for (let i = aliens.length -1; i > 0; i--) {
      if (aliens[i].y + (aliens[i].h / 2) >= 450) {
        return true;
      } else {
        return false;
      }
    }
  }
}