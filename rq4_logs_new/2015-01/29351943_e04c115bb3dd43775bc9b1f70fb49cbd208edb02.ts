function HSLA(value, offset, intensity = '100%', brightness = '50%', alpha = 1) {
    var hue = value % 360 + (offset || 0);
    return 'hsla(' + hue + ',' + intensity + ',' + brightness + ',' + alpha + ')';
}

function DrawLine(ctx: CanvasRenderingContext2D, a: Vector2, b: Vector2, color = 'white') {
    ctx.beginPath();
    ctx.moveTo(a.X, a.Y);
    ctx.lineTo(b.X, b.Y);
    ctx.strokeStyle = color;
    ctx.stroke();
    ctx.closePath();
}

function DrawCircle(ctx: CanvasRenderingContext2D, position: Vector2, radius = 1, color = 'white') {
    ctx.beginPath();
    ctx.arc(position.X, position.Y, radius, 0, 2 * Math.PI);
    ctx.strokeStyle = color;
    ctx.stroke();
    ctx.closePath();
}

function FillCircle(ctx: CanvasRenderingContext2D, position: Vector2, radius = 1, color = 'white') {
    ctx.beginPath();
    ctx.arc(position.X, position.Y, radius, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
    ctx.closePath();
}