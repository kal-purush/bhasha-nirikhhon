function FractalGraphics(interval, canvas) {
    this.graphics = new Graphics(canvas, 1, 1);

    // this.controlFrame = new ControlFrame(0, 0, 1, 1, "")
    //     .add();
    // this.graphics.addFrame(controlFrame);

    var kochSnowflake = new KochSnowflake(2);

    var fractalFrame = new Frame(0, 0, 1, 1, "")
        .add(kochSnowflake);
    this.graphics.addFrame(fractalFrame);


    this.graphics.drawFrames();
}

function KochSnowflake(iterations) {
    this.iterations = iterations;

    this.draw = function(width, height, context) {
        //var koch_flake = "FRFRF";
        function kochCurve(length, iteration) {
            if (iteration === 0) {
                context.lineTo(length);
            } else {
                kochCurve(length/3, iteration-1);
                right(60);
                context.rotate(-Math.PI / 3);
                kochCurve(length/3, iteration-1);
                context.rotate(2 * Math.PI / 3);
                kochCurve(length/3, iteration-1);
                context.rotate(-Math.PI / 3);
                kochCurve(length/3, iteration-1);
            }
        }

        kochCurve(width, iterations);

        context.stroke();

        // var koch_flake = "F";
        //
        // for (var i = 0; i < iterations; i++) {
        //     koch_flake = koch_flake.replace("F","FLFRFLF");
        // }
        //
        // koch_flake = koch_flake.split("");
        // console.log(koch_flake);
        //
        // var angle = 0;
        // var x = width / 2;
        // var y = height / 2;
        //
        // koch_flake.forEach(function(move) {
        //     if (move === "F") {
        //         dist = 100 / (Math.pow(3, (iterations - 1)));
        //
        //         x += Math.cos(angle) * dist;
        //         y += Math.sin(angle) * dist;
        //
        //         context.lineTo(x,y);
        //     } else  if (move === "L") {
        //         angle -= Math.PI / 3;
        //     } else if (move === "R") {
        //         angle += 2 * Math.PI / 3;
        //     }
        // }.bind(this));
        //
        // context.stroke();
    };
}

FractalGraphics.prototype.start = function() {
    this.graphics.drawFrames();
    setTimeout(this.start.bind(this), this.interval);
};

$(function() {
    var canvas = document.querySelector("#fractal canvas");
    var fractalGraphics = new FractalGraphics(1000, canvas);
    //fractalGraphics.graphics.drawFrames();
});