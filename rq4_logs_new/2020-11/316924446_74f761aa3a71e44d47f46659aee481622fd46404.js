leftWristX=0;
rightWristX=0;
difference=0;
function setup()
{canvas=createCanvas(550,500);
canvas.position(700,150);
video=createCapture(VIDEO);
video.position(60,150);
poseNet=ml5.poseNet(video,modelLoaded);
poseNet.on('pose',gotPoses);
}
function modelLoaded()
{
console.log("model loaded");
}
function gotPoses(results)
{
if(results.length>0)
{
console.log(results);
leftWristX=results[0].pose.leftWrist.x;
rightWristX=results[0].pose.rightWrist.x;
difference=floor(leftWristX-rightWristX);
console.log("leftWristX= "+leftWristX+"rightWrist= "+rightWristX+"difference= "+difference);
}
}
function draw()
{
background('#969A97');
fill('#F90093');
stroke('#F90093');
text("Samarth",100,150);
textSize(difference);
document.getElementById("size").innerHTML="size of the text is "+difference+"px";





}












