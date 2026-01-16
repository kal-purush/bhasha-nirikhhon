import { CanvasApp } from './../../utils/components/canvas-app';
import { DrawFactory } from './../../utils/factories/draw-factory';


/* 01 - drawingRectangles */
function drawingRectangles() {
    let canvasApp: CanvasApp = new CanvasApp(150, 150);
    let drawFactory: DrawFactory = new DrawFactory(canvasApp.canvas);
    canvasApp.title = "01 - drawingRectangles";
    drawFactory
        .fillRect(25, 25, 100, 100)
        .clearRect(45, 45, 60, 60)
        .strokeRect(50, 50, 50, 50);
}


/* 02 - drawingTriangle */
function drawingTriangle() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas);
    canvasApp.title = "02 - drawingTriangle";
    drawFactory.beginPath()
        .moveTo(75, 50)
        .lineTo(100, 75)
        .lineTo(100, 25)
        .fill();
}


/* 03- smile face */
function drawingSmileFace() {
    let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    canvasApp.title = "03 - smile face";
    drawFactory.beginPath()
        .arc(75, 75, 50, 0, Math.PI * 2, true)
        .moveTo(110, 75)
        .arc(75, 75, 35, 0, Math.PI, false)
        .moveTo(65, 65)
        .arc(60, 65, 5, 0, Math.PI * 2, true)
        .moveTo(95, 65)
        .arc(90, 65, 5, 0, Math.PI * 2, true)
        .stroke();
}


/* 04 - two triangles */
function drawingTwoTriangle() {
	let canvasApp = new CanvasApp(150, 150);
    let drawFactory = new DrawFactory(canvasApp.canvas)
    canvasApp.title = "04 - two triangles";
    // Filled triangle
    drawFactory.beginPath()
        .moveTo(25, 25)
        .lineTo(105, 25)
        .lineTo(25, 105)
        .fill();

    // Stroked triangle
    drawFactory.beginPath()
        .moveTo(125, 125)
        .lineTo(125, 45)
        .lineTo(45, 125)
        .closePath()
        .stroke();
}


/* Main */
function main() {
    drawingRectangles();  // 01
    drawingTriangle();  // 02
    drawingSmileFace();  // 03
    drawingTwoTriangle();  // 04
}
main()