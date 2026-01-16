class Application {
    constructor() {
    }

    public start() {
        var vis = new Visualizer(<HTMLCanvasElement>document.getElementById('canvas'));
        vis.init();
        vis.start();
    }
}

window.onload = () => {
    new Application().start();
}