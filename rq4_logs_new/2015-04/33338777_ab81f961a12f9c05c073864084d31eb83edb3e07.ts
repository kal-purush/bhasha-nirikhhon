var gl: WebGLRenderingContext;

class App {
    canvas: HTMLCanvasElement;
    context: WebGLRenderingContext;

    constructor(canvasId) {
        this.canvas = <HTMLCanvasElement>document.getElementById(canvasId);
        this.context = this.canvas.getContext("webgl");
        gl = this.context;
        gl.clearColor(0.1, 0.0, 0.0, 1.0);
        requestAnimationFrame(() => {
            this.run();
        });
    }

    run() {
        this.update(0);
        this.render();
    }

    update(dt: number) {

    }

    render() {
        gl.viewport(0, 0, this.canvas.width, this.canvas.height);

        gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT);

        
    }
}

window.onload = () => {
    var app: App = new App("game-canvas");
    app.run();
};