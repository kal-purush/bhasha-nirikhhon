function preload() {

}

function setup() {
     canvas=createCanvas(500,400);
     canvas.position(435,200);
     camera=createCapture(VIDEO);
     camera.hide();
     Myposenet=ml5.poseNet(camera,modelLoaded);
     Myposenet.on('pose',gotresults)
}

function draw() {
   image(camera,0,0,canvas.width,canvas.height);
}

function modelLoaded() {
     console.log("Model Loaded");
}

function gotresults(results) {
     nose_x=results[0].pose.nose.x;
     nose_y=results[0].pose.nose.y;
     console.log(nose_x,nose_y);
}