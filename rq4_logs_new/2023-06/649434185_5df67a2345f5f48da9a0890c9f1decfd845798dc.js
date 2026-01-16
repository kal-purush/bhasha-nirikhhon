class BulletWarrior{
    constructor(ctx, x, y, width, height, color, speed) {
        this.ctx = ctx;
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
        this.color = color;
        this.speed = speed;
        this.sound = new Audio();
        this.sound.src = './sound/disparoEnemy.wav';
      }
    
      draw() {
        this.ctx.fillStyle = this.color;
        this.ctx.fillRect(this.x, this.y, this.width, this.height);
      }
    
      move() {
        this.y += this.speed;
      }


    clear (){
        this.ctx.clearRect(this.x, this.y, this.width, this.height);
    }

      // colicion con el jugador
      
    collidesWith(player) {
      const warrior = player.x + player.width >= this.x &&
        player.x <= this.x + this.width &&
        player.y + player.height >= this.y &&
        player.y <= this.y + this.height;

        if (warrior) {
          this.sound.play();
        }
    
        return warrior;
    }

   



}