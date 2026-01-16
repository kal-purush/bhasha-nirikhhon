function setup() {
    createCanvas(500, 500);
  }
  
  function draw() {
    background(255);
    strokeWeight(1);
    noStroke();
    fill("#4b280a");
    arc(253,210,220,230,PI,0);
    quad(143,209,143,381,363,381,363,209);
    //#ffeacd
    fill("#fdece2");//머리
    arc(250,210,200,220,0,PI);
    arc(250,210,200,220,PI,0);
    
    stroke("#4b280a");
    fill("#4b280a");
    arc(250,220,210,240,radians(200),0);//머리
    arc(150,170,110,110,radians(30),radians(90));
    arc(367,160,120,190,radians(100),radians(135));
    
    fill("#fdece2");
    arc(250,210,100,100,radians(210),radians(330));//앞머리
    noFill();
    arc(150,170,110,110,radians(30),radians(90));
  
    line(207,185,198,197);
    fill("#fdece2");
    noStroke();
    triangle(207,185,180,220,280,220);
    triangle(293,185,320,220,200,220);
    line(293,185,309,210);
    
    stroke("#4b280a");
    arc(215,205,40,10,PI,0);//눈썹
    arc(285,205,40,10,PI,0);
    
    noStroke();
    fill(255);
    ellipse(215,225,40,22);//흰자
    ellipse(285,225,40,22);
    
    stroke(0);
    arc(215,225,40,22,PI,0);//눈꺼풀
    arc(285,225,40,22,PI,0);
    
    noFill();
    arc(215,225,50,22,radians(190),radians(270));//쌍꺼풀
    arc(285,225,50,22,radians(270),radians(350));
    
    fill("#4b280a")
    ellipse(215,225,15,20);//눈동자
    ellipse(285,225,15,20);
    
    fill(0);
    ellipse(215,225,10,10);
    ellipse(285,225,10,10);
    
    fill(255);
    ellipse(216,222,5,5);
    ellipse(286,222,5,5);
    
    noStroke();
    fill(255);
    quad(371,156,353,160,362,191,378,192);
    
    noStroke();
    fill("#fdece2");
    arc(145,225,10,10,PI,0);//귀
    arc(155,225,30,50,radians(80),PI);
    
    strokeWeight(5);//코
    stroke("#fce3cf");
    line(254,237,255,256)
    
    strokeWeight(1);
    fill("#F78181"); //입
    arc(250,270,40,20,0,PI);
    
    fill("#F6CECE")
    ellipse(300,255,20,10);//볼
    ellipse(200,255,20,10);
    
    /*fill()
    arc(250,410,300,120,PI,0);*/
    fill("#CED8F6");
    quad(206,350,294,350,294,500,206,500);//안에옷
    
    if (keyIsPressed) {
      if (key==='v'||key==='V') {
        stroke(200);
        noFill();
        arc(145,500,110,275,PI,radians(265));//왼팔
        arc(340,210,185,320,radians(10),radians(80));//오른팔위
        arc(350,210,250,480,radians(7),radians(90));//오른팔아래
        
        line(433,226,475,226);
        fill("#0B173B");
        quad(433,226,475,226,470,200,436,200);
      }
    }
    else {
      fill(255);
      stroke(200);
      arc(145,500,110,275,PI,radians(265));//왼팔
      arc(355,500,110,275,radians(270),0);//오른팔
    }
    
    noStroke();
    fill("#0B173B");
    arc(206,365,150,40,PI,radians(270));//남색본판
    arc(294,364,150,40,radians(270),0);
    quad(131,365,220,365,220,500,131,500)
    quad(369,364,280,364,280,500,369,500);
    
  
    noStroke();
    fill("#fdece2");//목
    quad(220,300,280,300,280,350,220,350);
    arc(250,350,60,20,0,PI);
    
    stroke("#fce3cf");
    strokeWeight(5);
    noFill();
    arc(250,210,200,220,radians(75),radians(105));
    
    noStroke();
    fill(255);
    arc(220,350,30,30,PI,radians(270));//카라
    arc(220,350,30,100,radians(90),PI);
    arc(280,350,30,30,radians(270),0);
    arc(280,350,30,100,0,radians(90));
    
    
    noFill(); //s
    stroke(255);
    strokeWeight(10);
    arc(325,430,35,35,PI,0);
    arc(325,430,35,35,radians(90),PI);
    arc(325,465,35,35,radians(270),0);
    arc(325,465,35,35,0,PI);
    
    fill(255);//단추
    point(290,418);
    point(290,470);
    
    noStroke();
    fill(0); 
    textSize(20);
    text("X: " + mouseX + " Y: " + mouseY, mouseX + 10, mouseY - 10);
   
  }
    
   
    
   