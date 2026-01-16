class Mover {
  constructor(x, y, radius) {
    //반지름에 맞춰서 질량을 맞추기
    //construsctor = 생산자x,y
    //쓰고 싶은 변수들을 여x,y기에 적는다
    this.pos = createVector(x, y);
    this.vel = createVector(0, 0);
    this.acc = createVector(0, 0);
    this.radius = radius;
    this.mass = radius ** (1 / 2); // 앞에 this는 반드시 들어가야함
  }

  applyForce(force) {
    // force.div(this.mass);
    let divedForce = p5.Vector.div(force, this.mass);
    this.acc.add(divedForce); // 이걸하면 업데이트에서 this.acc.mult(0)을 해야함
  }
  //속도에 가속도를 더하고 위치에 속도를 더한다
  update() {
    this.vel.add(this.acc);
    this.pos.add(this.vel);
    this.acc.mult(0); // 초기화, 이거 안하면 진짜 이상하게 움직임
  }

  edgeBounce() {
    if (this.pos.x < 0 + this.radius) {
      let delta = this.pos.x - (0 + this.radius);
      this.pos.x += -2 * delta;
      this.vel.x *= -1;
    } else if (this.pos.x > width - 1 - this.radius) {
      let delta = this.pos.x - (width - 1 - this.radius);
      this.pos.x += -2 * delta;
      this.vel.x *= -1;
    }
    if (this.pos.y > height - 1 - this.radius) {
      let delta = this.pos.y - (height - 1 - this.radius);
      this.pos.y += -2 * delta;
      this.vel.y *= -1;
    }
  }

  display() {
    ellipse(this.pos.x, this.pos.y, 2 * this.radius);
  }
}