// sketch 1
var sketchOne = function(p) {

    let buttonOne = document.getElementById("buttonOne");
    buttonOne.onclick = function() {
        p.setup();
        p.draw();
        buttonOne.style.textShadow = "none";
        setTimeout(function() { 
            buttonOne.style.textShadow = "2px 2px 4px black";
        }, 80);
    };

    const BORDER = 50
    let numTriangles = 0;
    let numClusters = 0;
    let colorPallete = [];
    let width = 500;
    let height = 500;

    p.setup = function() {
      const myCanvasOne = p.createCanvas(500, 500);
      myCanvasOne.parent("canvasOne");
      colorPallete = [p.random(255), p.random(255), p.random(255)];
      p.background(...colorPallete, 40);
      numTriangles = p.random(10,20);
      numClusters = p.random(5);
    }
  
    p.draw = function() {

        for (let c=0; c<numClusters; c++) {
            let centerx = p.random(BORDER, p.width-BORDER);
            let centery = p.random(BORDER, p.height-BORDER);
            let center2x = centerx + p.random(-100,100);
            let center2y = centery + p.random(-100,100);
        
            if (center2x < BORDER) { center2x = BORDER }
            else if (center2x > width-BORDER) { center2x = width-BORDER }
        
            if (center2y < BORDER) { center2y = BORDER }
            else if (center2y > height-BORDER) { center2y = height-BORDER }
        
            p.fill(colorPallete[0]+p.random(-40,40), colorPallete[1]+p.random(-40,40), colorPallete[2]+p.random(-40,40), p.random(20,100));
            p.noStroke();
        
            for (let i=0; i<numTriangles; i++) {
            p.triangle(centerx, centery, center2x, center2y, p.random(BORDER, width-BORDER), p.random(BORDER, height-BORDER));
            }

        }

        p.noLoop();
    }
  }
  
  // sketch 2
  var sketchTwo = function(p) {

    let buttonTwo = document.getElementById("buttonTwo");
    buttonTwo.onclick = function() {
        p.setup();
        p.draw();
        buttonTwo.style.textShadow = "none";
        setTimeout(function() { 
            buttonTwo.style.textShadow = "2px 2px 4px black";
        }, 80);
    };

    const ELEMENTSIZE = 25;
    let width = 500;
    let height = 500;
    const COLORS = [[50,89,146], [248,226,0], [135,96,99], [46,101,75], [78,124,19], [80,148,161]]

    p.setup = function() {
        const myCanvasTwo = p.createCanvas(500, 500);
        myCanvasTwo.parent("canvasTwo");
        p.rectMode(p.CENTER); 
        p.ellipseMode(p.CENTER);
        p.strokeWeight(ELEMENTSIZE/10);
    }
  
    p.draw = function() {
        for(i=ELEMENTSIZE/2; i<=width-ELEMENTSIZE/2; i+=ELEMENTSIZE) {
            for (j=ELEMENTSIZE/2; j<=height-ELEMENTSIZE/2; j+=ELEMENTSIZE) {
                
              p.noStroke();
              p.fill(32,34,36);
              p.rect(i,j,ELEMENTSIZE,ELEMENTSIZE);
              
              p.stroke(96, 102, 108);
              if (p.random() < 0.5) {
                p.arc(i-ELEMENTSIZE/2, j-ELEMENTSIZE/2, ELEMENTSIZE, ELEMENTSIZE, 0, p.HALF_PI);
                p.arc(i+ELEMENTSIZE/2, j+ELEMENTSIZE/2, ELEMENTSIZE, ELEMENTSIZE, p.PI, -p.HALF_PI);
              } else {
                p.arc(i+ELEMENTSIZE/2, j-ELEMENTSIZE/2, ELEMENTSIZE, ELEMENTSIZE, p.HALF_PI, p.PI);
                p.arc(i-ELEMENTSIZE/2, j+ELEMENTSIZE/2, ELEMENTSIZE, ELEMENTSIZE, -p.HALF_PI, 0);
              }
              
              if (p.random() < 0.2) {
                if (p.random() < 0.5) { let color = p.random(COLORS); p.stroke(color); p.fill(color) } 
                p.ellipse(i-ELEMENTSIZE/2,j-ELEMENTSIZE/2,ELEMENTSIZE/2);
              }
              
              if (i == width-ELEMENTSIZE/2) {
                if (p.random() < 0.2) {
                  if (p.random() < 0.5) { let color = p.random(COLORS); p.stroke(color); p.fill(color) } 
                  p.ellipse(i+ELEMENTSIZE/2,j-ELEMENTSIZE/2,ELEMENTSIZE/2);
                }
              }
              
              if (j == height-ELEMENTSIZE/2) {
                if (p.random() < 0.2) {
                  if (p.random() < 0.5) { let color = p.random(COLORS); p.stroke(color); p.fill(color) } 
                  p.ellipse(i-ELEMENTSIZE/2,j+ELEMENTSIZE/2,ELEMENTSIZE/2);
                }
              }
              
              if (i == width-ELEMENTSIZE/2 && j == height-ELEMENTSIZE/2) {
                if (p.random() < 0.2) {
                  if (p.random() < 0.5) { let color = p.random(COLORS); p.stroke(color); p.fill(color) } 
                  p.ellipse(i+ELEMENTSIZE/2,j+ELEMENTSIZE/2,ELEMENTSIZE/2);
                }
              }
            }
          }
          
          p.noLoop();
    }
  }

  // sketch 3
    var sketchThree = function(p) {

    let buttonThree = document.getElementById("buttonThree");
    buttonThree.onclick = function() {
        p.setup();
        p.draw();
        buttonThree.style.textShadow = "none";
        setTimeout(function() { 
            buttonThree.style.textShadow = "2px 2px 4px black";
        }, 80);
    };

    p.setup = function() {
        const myCanvasThree = p.createCanvas(500, 500);
        myCanvasThree.parent("canvasThree");
        p.background(255);
        p.strokeWeight(3);
        p.ellipseMode(p.CORNER);
    }
  
    p.draw = function() {
        for (let i=100; i<400; i+=150) {
            for (let j=100; j<400; j+=150) {
              p.noStroke(); p.fill(p.random(255), p.random(255), p.random(255), 155)
              if (p.random() < 0.5) { p.rect(i,j,150) } else { p.ellipse(i,j,150) }
              
              p.stroke(255); p.noFill()
              let sharedx = p.random(i+37.5,i+112.5), sharedy = p.random(j+37.5,j+112.5)
              for (let q=0; q<p.random(1,25); q++) {
                p.bezier(sharedx, sharedy, p.random(i,i+150), p.random(j,j+150), p.random(i,i+150), p.random(j,j+150), p.random(i+37.5,i+112.5), p.random(j+37.5,j+112.5))
              }
            }
          }
          
          p.noLoop();
    }
  }
  
  // sketch 4
  var sketchSix = function(p) {

    let buttonFour = document.getElementById("buttonSix");
    buttonSix.onclick = function() {
        p.setup();
        p.draw();
        buttonSix.style.textShadow = "none";
        setTimeout(function() { 
            buttonSix.style.textShadow = "2px 2px 4px black";
        }, 80);
    };

    p.setup = function() {
      const myCanvasSix = p.createCanvas(500, 500);
      myCanvasSix.parent("canvasSix");
      p.background(30,40,50)
      p.blendMode(p.EXCLUSION)
      p.angleMode(p.DEGREES)
    }
  
    p.draw = function() {
      p.strokeWeight(20)
      p.fill(255)
      p.fill(255,255,0)
      for(let i = 0 ; i < p.random(15,30); i++) {
        p.ellipse(p.random(500), p.random(500), p.random(60,140))
      }
      for(let i = 0 ; i < p.random(3); i++){
        star(p.random(500), p.random(500), p.random(0.5,3));
      }
      p.noLoop()
    }

    function star(x, y, s) {
      p.push()
      p.translate(x, y)
      p.scale(s)
      p.triangle(-5, 0, 5, 0, 0, -100)
      p.triangle(-5, 0, 5, 0, 0, 100)
      p.push()
      p.rotate(45)
      p.triangle(-5, 0, 5, 0, 0, -200)
      p.triangle(-5, 0, 5, 0, 0, 200)
      p.pop()
      p.push()
      p.rotate(90)
      p.triangle(-5, 0, 5, 0, 0, -100)
      p.triangle(-5, 0, 5, 0, 0, 100)
      p.pop()
      p.push()
      p.rotate(135)
      p.triangle(-5, 0, 5, 0, 0, -200)
      p.triangle(-5, 0, 5, 0, 0, 200)
      p.pop()
      p.pop()
    }
  }

  // sketch 5
var sketchFive = function(p) {

  let buttonFive = document.getElementById("buttonFive");
  buttonFive.onclick = function() {
      p.setup();
      p.draw();
      buttonFive.style.textShadow = "none";
      setTimeout(function() { 
          buttonFive.style.textShadow = "2px 2px 4px black";
      }, 80);
  };

  const BORDER = 0;
  const PIXELSIZE = [5,10,20,50];
  let q = {5:20000, 10:10000, 20:6000, 50:1000};
  let direction = 0;
  let pixelSize = 0;
  let colorway = 0;
  let width = 500;
  let height = 500;

  p.setup = function() {
    const myCanvasFive = p.createCanvas(500, 500);
    myCanvasFive.parent("canvasFive");
    p.background(0);
    p.frameRate(24);
    p.noStroke();
    p.ellipseMode(p.CORNER);
    pixelSize = p.random(PIXELSIZE);
    let xdirection = p.random([-1,0,1]);
    let ydirection = 0;
    if (xdirection == 0) { ydirection = p.random([-1,1]); }
    else { ydirection = p.random([-1,0,1]); }
    direction = [xdirection, ydirection];
    colorway = p.random([0,1,2,3]);
  }
  
  p.draw = function() {
    p.noStroke();
    for(i = BORDER+1; i < height-BORDER; i+=pixelSize) {
      for(j = BORDER+1; j < width-BORDER; j+=pixelSize) {
      
      var n = p.noise(i*0.005-direction[0]*p.millis()/q[pixelSize],j*0.005-direction[1]*p.millis()/q[pixelSize]);
      
      if (n < 0.65 && n >= 0.55) {
        
        if (colorway == 0) {
          p.fill(0,p.random(n*150,n*200),p.random(n*150,n*200));
        } else if (colorway == 1) {
          p.fill(p.random(n*350,n*450),0,p.random(n*350,n*450));
        } else if (colorway == 2) {
          p.fill(p.random(n*350,n*450),p.random(n*150,n*250),0);
        } else {
          p.fill(p.random(n*50,n*150));
        }
      
      } else if (n < 0.55 && n >= 0.5) {
        
        if (colorway == 0) {
          p.fill(p.random(n*450,n*500),p.random(n*450,n*500),0);
        } else if (colorway == 1) {
          p.fill(p.random(n*340,n*580),p.random(n*340,n*580),p.random(n*340,n*580));
        } else if (colorway == 2) {
          p.fill(0,p.random(n*100,n*150),p.random(n*350,n*450));
        } else {
          p.fill(p.random(n*200,n*300));
        }
        
      } else {
        p.fill(0);
      }
        p.square(i,j,pixelSize);
      }
    }
  }
}
  
  // sketch 6
  var sketchFour = function(p) {

    buttonFour.onclick = function() {
      p.setup();
      buttonFour.style.textShadow = "none";
      setTimeout(function() { 
          buttonFour.style.textShadow = "2px 2px 4px black";
      }, 80);
  };

    p.setup = function() {
      const myCanvasFour = p.createCanvas(500, 500);
      myCanvasFour.parent("canvasFour");

      let rects = [];
      for (let i = 0; i < 20; i++) {
        let rectWidth = p.random(50, 150);
        let rectHeight = p.random(50, 200);
        let rectX = p.random(20, 500 - rectWidth - 20);
        let rectY = p.random(20, 500 - rectHeight - 20);
        let overlapping = true;  

      let safety = 0;
      while (overlapping) {
        overlapping = false;
        for (let j = 0; j < rects.length; j++) {
          if (rectX + rectWidth + 20 > rects[j].x && rectX - 20 < rects[j].x + rects[j].width && 
            rectY + rectHeight + 20 > rects[j].y && rectY - 20 < rects[j].y + rects[j].height) {
            rectX = rectX = p.random(20, 500 - rectWidth - 20);
            rectY = rectY = p.random(20, 500 - rectHeight - 20);
            if (safety < 500) { 
              safety++;
              overlapping = true;
              break;
            }
          }
        }
      }

      if (safety < 100) {
        rects.push({x: rectX, y: rectY, width: rectWidth, height: rectHeight});
      }
    }

    p.background(200);
    for (let i = 0; i < rects.length; i++) {
      p.stroke(p.random(25,225), p.random(25,225), p.random(25,225));
      p.strokeWeight(5);
      p.fill(255);
      p.rect(rects[i].x, rects[i].y, rects[i].width, rects[i].height);
      p.strokeWeight(0)
      p.fill(22);
      p.rect(rects[i].x + 10, rects[i].y + 10, rects[i].width - 20, rects[i].height - 20);
    }
  }
}

  var mySketchOne = new p5(sketchOne);
  var mySketchTwo = new p5(sketchTwo);
  var mySketchThree = new p5(sketchThree);
  var mySketchFour = new p5(sketchFour);
  var mySketchFive = new p5(sketchFive);
  var mySketchSix = new p5(sketchSix);