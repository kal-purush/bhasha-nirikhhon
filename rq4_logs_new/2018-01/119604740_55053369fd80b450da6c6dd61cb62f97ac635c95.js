// const square = document.getElementById('square').value = '';
const circle = document.getElementById('circleInput');
const circleButton = document.getElementById('newCircle')
// const triangle = document.getElementById('triangle').value = '';
// const recWidth = document.getElementById('recWidth').value = '';
// const recHeight = document.getElementById('recHeight').value = '';
const posShape = document.getElementById('shape-display');
var posx = Math.floor(Math.random()*601);
var posy = Math.floor(Math.random()*601);



document.addEventListener('DOMContentLoaded', function(){
    shapeContainer = document.getElementsByClassName('shapeContainer');
    

});
    

class Shape {
    constructor(color,type){
        this.color = color;
        this.type = type;
    }
    render(){

    }
}

class CircleSHAPE extends Shape{
    constructor (radius){
        super("purple","circle");
        this.radius = radius;
        
    }
    createCircle (){
        const makeCircle = document.createElement("div");
        makeCircle.className = 'circle';
        makeCircle.style.height = `${this.radius * 2}px`;
        makeCircle.style.width = `${this.radius * 2}px`;
        posShape.appendChild(makeCircle);
        
    }

    
}

circleButton.addEventListener('click',function(){
    let newCircle = new CircleSHAPE(circle);
    newCircle.createCircle();


})

class Square extends Shape {
    constructor(sideLength) {
        this.sideLength = square.value;
        super("red")
    }

}
//triangle holds height(which is the same as width/base)
class Triangle extends Shape{
    constructor(height) {
    this.height = triangle.value;
        super("yellow")
    }

    // randomXY(){
    //     console.log('check')
    // }
}

class Rectangle extends Shape{
    constructor(width,height) {
        this.width = recWidth.value;
        this.height = recHeight.value;
        super("green")
    }
}

// function randomXY(){
//     document.getElementsByClassName('randomPosition').css({
//      'position':'absolute',
//      'left':posx + 'px',
//      'top':posy + 'px'
//  })
//  }

 


 




// function new circle(){

// }

// function new square(){

// }

// function new triangle(){

// }
// function new recangle(){

// }