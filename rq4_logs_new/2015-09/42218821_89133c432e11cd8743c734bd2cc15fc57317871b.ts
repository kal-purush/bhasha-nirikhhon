function makeInput(id, callbackOnChange, min, max) {
    var el: any = document.getElementById(id)
    if (!el) {
        console.error(id + " doesn't exist")
        return;
    }
    el.addEventListener("change", function(evt) {
        callbackOnChange((<HTMLTextAreaElement> evt.srcElement).value);
        (new Renderer).render();
    })
    el.min = min || "";
    el.max = max || "";
}
function update(id, val){
    var el: any = document.getElementById(id)
    if (!el)
        console.error(id + " doesn't exist")
    else
        el.value = el.val = val;
}

class Complex {
    constructor(public r: number, public i: number) { }
    plusBy(c: Complex) {
        this.r += c.r;
        this.i += c.i;
        return this;
    }
    timesBy(c: Complex) {
        //(a+bi)(c+di)  == (ac-bd) + (ad + bc)i
        var r = this.r * c.r - this.i * c.i;
        var i = this.r * c.i + this.i * c.r;
        this.r = r;
        this.i = i;
        return this;
    }
    squareThenPlus(c) {
        var r = this.r * this.r - this.i * this.i;
        var i = this.r * this.i + this.i * this.r;
        this.r = r + c.r;
        this.i = i + c.i;
        return this;
    }
    abs(c: Complex) { return Math.sqrt(this.r * this.r + this.i * this.i) }
}

function julia(z, c, maxIterations) {
    for (var n = 1; n < maxIterations; n++)
        if (z.squareThenPlus(c).abs() > 16)
            return n - 1
    return maxIterations;
}

function julia2(z: Complex, c: Complex, maxIterations) {
    var zr = z.r;
    var zi = z.i;
    var iterations = 0;

    var zrNext, ziNext;
    while (true) {
        iterations++;
        if (iterations > maxIterations) return maxIterations;
        zrNext = zr * zr - zi * zi + c.r;
        ziNext = 2 * zi * zr + c.i;
        zr = zrNext;
        zi = ziNext;
        if (zr > 4 || zi > 4) return iterations;
    }
    return iterations;
}

//julia set param
var width = 500,
    height = 500,
    iLimit = 1,
    rLimit = 1,
    xIncr = 1,
    yIncr = 1,
    constantR = -.06,
    constantI = .67,
    numBuffered = 10,
    centerX = 0,
    centerY = 0,
    zoom = 1,
    iterations = 100;

makeInput("sizeX", val => { height = +val }, 0, 1000);
makeInput("sizeY", val => { width = +val }, 0, 1000);
makeInput("iLimit", val => { iLimit = +val }, -1, 1);
makeInput("rLimit", val => { rLimit = +val }, -1, 1);
makeInput("xIncr", val => { xIncr = +val }, 1, 10);
makeInput("yIncr", val => { yIncr = +val }, 1, 10);
makeInput("constantR", val => { constantR = +val }, -1, 1);
makeInput("constantI", val => { constantI = +val }, -1, 1);
makeInput("iterations", val => { iterations = +val }, 0, 500);
makeInput("centerX", val => { centerX = +val }, -1000, 1000);
makeInput("centerY", val => { centerY = +val }, -1000, 1000);
makeInput("zoom", val => { zoom = +val }, .25, 20);

function updateAll(){
    update("sizeX",width);
    update("sizeY",height);
    update("iLimit",iLimit);
    update("rLimit",rLimit);
    update("xIncr",xIncr);
    update("yIncr",yIncr);
    update("constantR",constantR);
    update("constantI",constantI);
    update("iterations",iterations);
    update("centerX",centerX);
    update("centerY",centerY);
    update("zoom",zoom);
}
class Renderer {
    canvas = <HTMLCanvasElement> document.getElementById("fractal");
    ctx = this.canvas.getContext("2d");

    xTor = rLimit * 2 / width;
    yToi = iLimit * 2 / height;

    constructor() {
        updateAll();
        this.canvas.width = width;
        this.canvas.height = height;
        this.canvas.addEventListener("mousedown", this.mousedown.bind(this))
        document.addEventListener("click", this.click.bind(this));
        document.addEventListener("mousemove", this.mousemove.bind(this));
    }

    _zoom(centerX, centerY, zoom, qualityDecr?) {
        qualityDecr = Math.round(qualityDecr || 1);
        this._render(0, 0, -centerX, -centerY, width, height, xIncr * qualityDecr, yIncr * qualityDecr, this.xTor / zoom, this.yToi / zoom)
    }

    startingX = 0;
    startingY = 0;

    dragging = false;
    _mousedown: () => any;
    mousedown(e: MouseEvent) {
        this.dragging = true;
        this.startingX = e.x;
        this.startingY = e.y;
    }

    mousemove(e: MouseEvent) {
        if(this.dragging){
            centerY -= this.startingY - e.y;
            centerX -= this.startingX - e.x;

            this.startingX = e.x;
            this.startingY = e.y;

            this._zoom(centerX, centerY, zoom, 8)

        }
    }

    click(e: MouseEvent) {
        document.removeEventListener("mousemove", this.mousemove);
        document.removeEventListener("click", this.click);
        this.startingX = null;
        this.startingY = null;
        this.dragging = false;
        this.render()
    }

    _render(startX, startY, centerX, centerY, endX, endY, stepX, stepY, xTor, yToi) {
        var iteratingComplex = new Complex(0, 0);
        var constant = new Complex(constantR, constantI);
        for (var x = startX; x < endX; x += stepX)
            for (var y = startY; y < endY; y += stepY) {
                iteratingComplex.r = xTor * ((centerX + x) - width / 2)
                iteratingComplex.i = yToi * ((centerY + y) - height / 2)
                this.ctx.fillStyle = "hsl(" + Math.round(255 * (julia2(iteratingComplex, constant, iterations) / iterations))
                + ", 100%, 50%)";
                this.ctx.fillRect(x, y, stepX, stepY);
            }
    }
    render() {
        console.time("fastRender")
        this._zoom(centerX, centerY, zoom, 8)
        console.timeEnd("fastRender")

        setTimeout(() => {
            console.time("fullRender")
            this._zoom(centerX, centerY, zoom)
            console.timeEnd("fullRender")
            console.error("Finished rendering at ", centerX, centerY)
        }, 0)
    }
}
function debounce(fn, delay) {
    var timer = null;
    return function() {
        var context = this, args = arguments;
        clearTimeout(timer);
        timer = setTimeout(function() {
            fn.apply(context, args);
        }, delay);
    };
}
(new Renderer).render();