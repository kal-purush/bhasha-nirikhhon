function startGame() {
    document.write("hellosup");
    var canvas = document.createElement("canvas");
    canvas.width=300
    canvas.height=150
    canvas.style="border:1px solid #d3d3d3;"
    document.body.insertBefore(canvas, document.body.childNodes[0]);
    //var canvas = document.getElementById("canvas");
    var context = canvas.getContext("2d");
    context.clearRect(0,0, 480, 270);
    context.rect(20,20,150,100);
    context.stroke();
    context.fillStyle = "red";
    context.fillRect(20,20,30,30);
}

function gameObject(width, height, color, x, y) {
    this.width = width;
    this.height = height;
    this.color = color;
    this.x = x;
    this.y = y;
    this.velocity_x = 0;
    this.velocity_y = 0;

}