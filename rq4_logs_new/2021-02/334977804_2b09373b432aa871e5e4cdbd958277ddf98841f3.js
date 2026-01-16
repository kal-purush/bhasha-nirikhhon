var canvas = document.getElementById("canvas");
var ctx = canvas.getContext("2d");
canvas.width = window.innerWidth;
canvas.height = window.innerHeight;
var particles = [];
var num_particles = 40000;//MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM MICROWAVE TIME

//Helps prevent colors from being too high or too low
function GetRandomColor() {
    var r = 0, g = 0, b = 0;
    while (r > 100 && g > 100 && b > 100)
    {
        r = Math.floor(0);
        g = Math.floor(0);
        b = Math.floor(0);
    }

    return "rgb(" + r + "," + g + ","  + b + ")";
}
//Particle object with random starting position, velocity and color
var Particle = function () {
    this.x = canvas.width * Math.random();
    this.y = canvas.height * Math.random();
    this.vx = 200 * Math.random() - 100;//x axis
    this.vy = 200 * Math.random() - 120;//y axis
    this.Color = GetRandomColor();
}
//Ading two methods
Particle.prototype.Draw = function (ctx) {
    ctx.fillStyle = this.Color;
    //change fuzzy boi size
    ctx.fillRect(this.x, this.y, 5, 7);
}
//................................................................................................................................        ya know im just typing things here cuz i ran outa things to do and now im bored lol i just ran outta things to do fam also if ur reading this send an email to me saying hi its guppyexpress@gmail.com                                                                                                                                                                                                                          OwO wats this? an eastew egg? i hope you enjoyed my website its, made fwom puwe bowdom and has no actuaw puwpose, it is just that feewing u get in uw weg when u sit down fow too wong, ow the taste of spawkwing watew, ow awso micwowave noises, haha weww enjoy :D
Particle.prototype.Update = function () {
    this.x += this.vx;
    this.y += this.vy;
 
    if (this.x<0 || this.x > canvas.width)
        this.vx = -this.vx;
 
    if (this.y < 0 || this.y > canvas.height)
        this.vy = -this.vy;
}
function loop() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    for (var i = 0; i < num_particles; i++) {
        particles[i].Update();
        particles[i].Draw(ctx);
    }
    requestAnimationFrame(loop);
}
//Create particles
for (var i = 0; i < num_particles; i++)
    particles.push(new Particle());
loop();