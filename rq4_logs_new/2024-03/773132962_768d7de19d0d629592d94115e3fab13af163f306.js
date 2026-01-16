img = "";
status = "";

function preload(){
    img = loadImage("fruit.jpg");
}

function setup() {
    canvas = createCanvas(640, 420);
    canvas.center();
    object = ml5.objectDetector('cocossd', modelLoaded);
    document.getElementById("status").innerHTML = "Status : Detecting Object";
}

function draw() {
    image(img, 0, 0, 640, 420);
    fill("red");
    text("apple", 45, 75);
    noFill();
    stroke("red");
    rect(30, 60, 300, 300 );

    fill("red");
    text("orange", 320, 120);
    noFill();
    stroke("red")
    rect(300, 90, 270, 250 );
}

function modelLoaded() {
    console.log("Model Loaded!")
    status = true;
    object.detect(img, gotResult);
}

function gotResult(error, results) {
    if (error) {
        console.log(error);
    }
    console.log(results);
}