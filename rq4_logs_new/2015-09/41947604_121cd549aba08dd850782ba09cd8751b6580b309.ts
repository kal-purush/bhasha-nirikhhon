var N_x = 100;
var dt = 1/60;
var dx = 1;
var c = 5;
var alpha = Math.pow(c * dt / dx, 2);

function main() {
    var canvas = <HTMLCanvasElement>document.getElementById("canvas");
    canvas.width = window.innerHeight;
    canvas.height = window.innerWidth;
    var ctx = canvas.getContext("2d");
    var y = [];
    for (var i = 0; i < N_x; i++) {
        y[i] = f(i);
    }
    var prev_y: Array<number> = null;
    draw(ctx, y);
    window.onclick = () => {
        setInterval(function () {
            var new_y = next(y, prev_y);
            draw(ctx, new_y);
            prev_y = y;
            y = new_y;
        }, dt * 1000);
    };
}

function draw(ctx: CanvasRenderingContext2D, y: Array<number>) {
    ctx.fillStyle = "gray";
    ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);
    ctx.strokeStyle = "white";
    ctx.save();
    ctx.translate(0, ctx.canvas.width / 2);
    ctx.scale(1, -1);
    ctx.beginPath();
    ctx.moveTo(0, 100 * y[0]);
    var step = ctx.canvas.width / N_x;
    for (var i = 0; i < N_x; i++) {
        ctx.lineTo(step * i, 100 * y[i]);
    }
    ctx.stroke();
    ctx.restore();

}

// y(0, x)
function f(x: number) {
    //return x / N_x;
    //return Math.sin(x / N_x * 10);
    //return Math.random();
    //return 0;
    if (Math.abs(x - N_x / 2) < 3) {
        return 1;
    } else {
        return 0;
    }
}

// ∂y/∂t(0, x)
function g(x: number) {
    return 0;
}

// y(1, x)
function y1(x: number) {
    if (x == 0) {
        //return 0;
        return f(x) + g(x) * dt + alpha / 2 * (f(x + 1) + f(x + 1) - 2 * f(x));
    } else if (x == N_x - 1) {
        //return 0;
        return f(x) + g(x) * dt + alpha / 2 * (f(x - 1) + f(x - 1) - 2 * f(x));
    } else {
        return f(x) + g(x) * dt + alpha / 2 * (f(x + 1) + f(x - 1) - 2 * f(x));
    }
}

// y(t, x)
function nextvalue(x: number, prev: Array<number>, prevprev: Array<number>) {
    if (x == 0) {
        //return 0;
        return 2 * prev[x] - prevprev[x] + alpha * (prev[x + 1] + prev[x + 1] - 2 * prev[x]);
    } else if (x == N_x - 1) {
        //return 0;
        return 2 * prev[x] - prevprev[x] + alpha * (prev[x - 1] + prev[x - 1] - 2 * prev[x]);
    } else {
        return 2 * prev[x] - prevprev[x] + alpha * (prev[x + 1] + prev[x - 1] - 2 * prev[x]);
    }
}

function next(prev: Array<number>, prevprev: Array<number>) {
    var y = [];
    for (var x = 0; x < N_x; x++) {
        if (prevprev == null) {
            y[x] = y1(x);
        } else {
            y[x] = nextvalue(x, prev, prevprev);
        }
    }
    return y;
}

window.onload = main;