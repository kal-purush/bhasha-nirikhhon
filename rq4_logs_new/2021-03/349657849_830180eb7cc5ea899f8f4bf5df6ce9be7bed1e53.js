objects = []
status = ""

function preload() {

}

function setup() {
   canvas = createCanvas(380, 380)
   canvas.center()
   
   video = createCapture(VIDEO)
   video.size(380 , 380)
   video.hide()

   objectdetector = ml5.objectDetector("cocossd", modeloaded)
}

function modeloaded() {
   console.log("Model is working!")
   status = true
   document.getElementById("status").innerHTML = "Status : detecting objects";
  
}

function gotresults(error, results) {
   if (error) {
      console.log(error)
   }
   else{
      console.log(results)
      objects = results
   }
}

function draw() {
   image(video, 0, 0, 380, 380)
   
   if (status != "") {
      objectdetector.detect(video, gotresults)
      for (let i= 0; i < objects.length; i++) {
         document.getElementById("num").innerHTML ="Number of objects detected = "+objects.length
         document.getElementById("status").innerHTML="Status:Objects Detected"
         object_name = objects[i].label
         x = objects[i].x
         y = objects[i].y
         width = objects[i].width
         height = objects[i].height
         acc = floor(objects[i].confidence * 100)+"%"

         r = random(255)
         g = random(255)
         b = random (255)

         fill(r,g,b)
         text(object_name+" "+acc , x , y - 7)
         textSize(15)
         noFill()
         stroke(r,g,b)
         strokeWeight(1)
         rect(x,y,width,height)

      }
   }
}