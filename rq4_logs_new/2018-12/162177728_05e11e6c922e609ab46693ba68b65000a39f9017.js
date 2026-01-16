
var myCanvas;
var myContext;
function init(){
    myCanvas = document.querySelector("canvas");
    myContext = myCanvas.getContext("2d");
    myContext.fillStyle = "yellow";
    myContext.arc(300, 300, 10, 0, 2 * Math.PI);
    myContext.fill()
    myCanvas.addEventListener("mousemove",function(event) {
        myFunction(event);
    });
    
    
    
}

function myFunction(e){
    var x = e.clientX;
    var y = e.clientY;
    var red=Math.round(Math.random() * 255);
    var green=Math.round(Math.random() * 255);
    var blue=Math.round(Math.random() * 255);
    myContext.beginPath()
    console.log(myContext.fillStyle = "rgb("+red  +","+ green +"," + blue + ")");
    myContext.arc(x, y, 10, 0, 2 * Math.PI);
    myContext.fill()
    console.log(x, y)
}