lipstickX = 0;
lipstickY = 0;

function preload() {
 lipstick = loadImage('lipstick.jpeg');
}

function draw() {
    image(video, 0, 0, 300, 300);
    image(lipstick, lipstickX, lipstickY, 30, 30);

}

function setup() {

    canvas = createCanvas(300,300);
    canvas.center();
    video = createCapture(VIDEO);
    video.size(300,300);
    video.hide();

    poseNet = ml5.poseNet(video, modelLoaded);
    poseNet.on('pose', gotPoses);
}

function modelLoaded() {

    console.log('Pose Net is Initialized');
}

function gotPoses(results) {

    if(results.length > 0) {
        console.log(results);
        lipstickX = results[0].pose.nose.x-25;
        lipstickY = results[0].pose.nose.y+15;
        console.log("Lipstick x = " + lipstickX);
        console.log("Lipstick y = " + lipstickY);
    }
}

function take_snapshot() {

    save('my_filter.png');
}