var mouseevent="empty";
var last_position_of_x,last_position_of_y;
canvas=document.getElementById("myCanvas");
ctx=canvas.getContext("2d");
color="pink";
width_of_line=3;
canvas.addEventListener("mousedown", pendown);
function pendown(e){
    mouseevent="mousedown";
}

canvas.addEventListener("mouseup", penup);
function penup(e){
    mouseevent="mouseup";
}

canvas.addEventListener("mouseleave", penleave);
function penleave(e){
    mouseevent="mouseleave";
}

canvas.addEventListener("mousemove", penmove);
function penmove(e){
    current_position_of_mouse_x=e.clientX-canvas.offsetLeft;
    current_position_of_mouse_y=e.clientY-canvas.offsetTop;
    if(mouseevent=="mousedown"){
        ctx.beginPath();
        ctx.strokeStyle=color;
        ctx.lineWidth=width_of_line;

        console.log("last position of x and y coordinates=");
        console.log("x="+ last_position_of_x+"y="+ last_position_of_y);
        ctx.moveTo(last_position_of_x,last_position_of_y);

        console.log("current position of x and y coordinates=");
        console.log("x="+ current_position_of_mouse_x+"y="+ current_position_of_mouse_y);
        ctx.lineTo(current_position_of_mouse_x,current_position_of_mouse_y);
        ctx.stroke();

 
    }
    last_position_of_x=current_position_of_mouse_x;
    last_position_of_y=current_position_of_mouse_y
}