/*Aufgabe: (Nummer 7a)
Name: (Sara Kalinic)
Matrikel: (255073)
Datum: (07.05.17)
Hiermit versichere ich, dass ich diesen
Code selbst geschrieben habe. Er wurde
nicht kopiert und auch nicht diktiert. */

/* Die Regenbogenflügel sind geplant gewesen. */

namespace L6_Classes {
    window.addEventListener("load", init);
   export let crc2: CanvasRenderingContext2D;
    
   let bee: BieneData[] = [];
    let n: number = 10;
    let image: ImageData;

    function init(_event: Event): void {
        let canvas: HTMLCanvasElement;
        canvas = document.getElementsByTagName("canvas")[0];
        canvas.width = 1920;
        canvas.height = 1080;
        console.log(canvas);

        crc2 = canvas.getContext("2d");
        console.log(crc2);

        crc2.fillStyle = "#ccffff";
        crc2.fillRect(0, 0, canvas.width, canvas.height);

        //Wolke + Himmel
        drawCloud(0, 100, 100, 180, 370, "#f2f2f2", "#f2f2f2");
        drawCloud(350, 60, 100, 180, 370, "#ffe6f2", "#ffe6f2");
        drawCloud(900, 50, 100, 180, 370, "#ffe6f2", "#ffe6f2");
        drawCloud(170, 0, 100, 180, 370, "#ffffff", "#ffffff");
        drawCloud(570, 20, 100, 180, 370, "#f2f2f2", "#f2f2f2");
        drawCloud(1600, 20, 100, 180, 370, "#ffe6f2", "#ffe6f2");
        drawCloud(1290, 70, 100, 180, 370, "#f2f2f2", "#f2f2f2");
        drawCloud1(770, 150, 100, 180, 70, "#ffffff", "#ffffff");
        drawCloud1(880, 0, 100, 180, 70, "#f2f2f2", "#f2f2f2");
        drawCloud1(1080, 0, 100, 180, 70, "#ffffff", "#ffffff");
        drawCloud1(1500, 150, 100, 180, 70, "#ffffff", "#ffffff");
        drawCloud1(1760, 10, 100, 180, 70, "#ffffff", "#ffffff");
        drawSun(0, 0, 100, 180, 270, "#ffffff", "#ffff99");

           //Berge
        drawBerg(2000, 540, "#cccccc", "#cccccc");
        drawBerg1(1800, 540, "#bfbfbf", "#bfbfbf");
        drawBerg2(1900, 540, "#d9d9d9", "#d9d9d9");
        drawBerg(1500, 540, "#cccccc", "#cccccc");
        drawBerg1(1300, 540, "#bfbfbf", "#bfbfbf");
        drawBerg2(1400, 540, "#d9d9d9", "#d9d9d9");
        drawBerg(1000, 540, "#cccccc", "#cccccc");
        drawBerg1(900, 540, "#bfbfbf", "#bfbfbf");
        drawBerg2(700, 540, "#d9d9d9", "#d9d9d9");
        drawBerg(500, 540, "#cccccc", "#cccccc");
        drawBerg1(600, 540, "#bfbfbf", "#bfbfbf");
        drawBerg2(700, 540, "#d9d9d9", "#d9d9d9");

        //Wiesen
        drawWiese3(0, 473, "#2d8659", "#2d8659");
        drawFluss(1100, 473, "#004d00", "#99e6ff", " #cceeff", "#cceeff");
        drawWiese1(1920, 473, "#339966", "#339966");
        drawWiese2(0, 540, "#adebad", "#adebad");

        //Haus
        drawHaus(0, 540, "#734d26", "#d9b38c", "#663300", "#cc9966");
        drawTure(0, 540, "#4d2600", "#ac7339");
        drawFenster(0, 540, "#4da6ff", "#cce6ff");
        drawFenster(165, 540, "#4da6ff", "#cce6ff");
        drawBank(20, 700, "#734d26", "#d9b38c", "#4d2600", "#4d2600");
        drawStraße(0, 540, "#4d4d4d", "#999999");
        drawBusch( 0, 540, 100, 180, 270, "#339966", "#339966");
        drawBusch1( 190, 540, 100, 180, 270, "#339966", "#339966");
      
        drawKorb(1620,  940, "#663300", "#663300", "#000000", "#000000");
       

        //Blume zufällig platzieren
        
        
        // Hintergrundbild abspeichern
        image = crc2.getImageData(0, 0, canvas.width, canvas.height);
         
        //Startposition der Bienen 
        for (let i: number = 0; i < n; i++) {
            let b: BieneData = new BieneData ();
            b.setStart();
            bee[i] = b; 
        }
        
        window.setTimeout(animate, 20);
        
        // Eventlistener -> bei Klick neue Biene
        canvas.addEventListener("touch", drawNeueBiene); 
        canvas.addEventListener("click", drawNeueBiene);
            } 
  
     //Wolken und Sonne
     function drawCloud(_x: number, _y: number, _x1: number, _y1: number, r: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.arc(_x, _y, 100, 180, 470);
        crc2.arc(_x + 100, _y - 50, 100, 180, 470);
        crc2.arc(_x + 120, _y + 50, 100, 180, 270);
        crc2.arc(_x + 270, _y, 100, 180, 470);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Wolken zeichnen 
    function drawCloud1(_x: number, _y: number, _x1: number, _y1: number, r: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.arc( _x, _y, 50, 0, 360);
        crc2.arc(_x + 50, _y + 30, 50, 0, 360);
        crc2.arc(_x + 110, _y + 30, 50, 0, 360);
        crc2.arc(_x + 70, _y - 20, 60, 0, 360);
        crc2.arc(_x + 140, _y, 50, 0, 360);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Sonne zeichnen
    function drawSun(_x: number, _y: number, _x1: number, _y1: number, r: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.arc(_x, _y, 140, 180, 270);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Wiese 1 zeichnen
    function drawWiese1(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.lineTo(_x - 1920, _y + 470);
        crc2.lineTo(_x, _y + 470);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Wiese 2 zeichnen 
    function drawWiese2(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.lineTo(_x + 300, _y);
        crc2.lineTo(_x + 1920, _y + 400);
        crc2.lineTo(_x + 1920, _y + 540);
        crc2.lineTo(_x, _y + 540);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // wiese 3 zeichnen 
    function drawWiese3(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.lineTo(_x + 1920, _y);
        crc2.lineTo(_x + 1920, _y + 400);
        crc2.lineTo(_x + 1920, _y + 540);
        crc2.lineTo(_x, _y + 540);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // BERGE 
    function drawBerg(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 30, _y);
        crc2.lineTo(_x - 150, _y - 250);
        crc2.lineTo(_x - 300, _y);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    
    // Berg 1 zeichnen
    function drawBerg1(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 30, _y);
        crc2.lineTo(_x - 180, _y - 300);
        crc2.lineTo(_x - 360, _y);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Berg 2 zeichnnen
    function drawBerg2(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 30, _y);
        crc2.lineTo(_x - 150, _y - 200);
        crc2.lineTo(_x - 300, _y);
        crc2.lineTo(_x, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Haus
    function drawHaus(_x: number, _y: number, _strokeColor: string, _fillColor: string, _strokeColor1: string, _fillColor1: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.lineTo(_x + 300, _y);
        crc2.lineTo(_x + 300, _y - 200);
        crc2.lineTo(_x + 150, _y - 300);
        crc2.lineTo(_x, _y - 200);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();

        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x, _y - 200);
        crc2.lineTo(_x + 300, _y - 200);
        crc2.lineTo(_x + 150, _y - 300);
        crc2.lineTo(_x, _y - 200);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Türe zeichnen
    function drawTure(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 125, _y);
        crc2.lineTo(_x + 175, _y);
        crc2.lineTo(_x + 175, _y - 60);
        crc2.lineTo(_x + 125, _y - 60);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Straße zeichnen
    function drawStraße(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 175, _y);
        crc2.lineTo(_x + 175, _y);
        crc2.lineTo(_x + 175, _y + 60);
        crc2.lineTo(_x + 179, _y + 68);
        crc2.lineTo(_x + 350, _y + 270);
        crc2.lineTo(_x + 200, _y + 340);
        crc2.lineTo(_x, _y + 540);
        crc2.lineTo(_x, _y + 480);
        crc2.lineTo(_x + 180, _y + 290);
        crc2.lineTo(_x + 270, _y + 240);
        crc2.lineTo(_x + 130, _y + 70);
        crc2.lineTo(_x + 125, _y);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }

    // Fenster zeichnen
    function drawFenster(_x: number, _y: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 50, _y - 100);
        crc2.lineTo(_x + 80, _y - 100);
        crc2.lineTo(_x + 80, _y - 130);
        crc2.lineTo(_x + 50, _y - 130);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
    }
    
    // Busch zeichnen
      function drawBusch(_x: number, _y: number, _x1: number, _y1: number, r: number, _strokeColor: string, _fillColor: string): void {
          crc2.beginPath();
          crc2.fillStyle = _fillColor;
          crc2.strokeStyle = _strokeColor;
          crc2.moveTo(_x, _y);
          crc2.arc(_x, _y, 15, 180, 270);
          crc2.moveTo(_x, _y);
          crc2.arc(_x + 10, _y - 10, 15, 180, 270);
          crc2.moveTo(_x, _y);
          crc2.arc(_x + 20, _y - 10, 15, 180, 270);
          crc2.moveTo(_x, _y);
          crc2.arc(_x + 30, _y, 15, 180, 270);
          crc2.moveTo(_x, _y);
          crc2.arc(_x + 20, _y + 5, 15, 180, 270);
          crc2.closePath();
          crc2.fill();
          crc2.stroke();
          
          crc2.beginPath();
          crc2.fillStyle = _fillColor;
          crc2.strokeStyle = _strokeColor;
          crc2.moveTo(_x + 40, _y + 25);
          crc2.arc(_x + 40, _y + 25, 15, 180, 270);
          crc2.moveTo(_x + 50, _y + 15);
          crc2.arc(_x + 50, _y + 15, 15, 180, 270);
          crc2.moveTo(_x + 65, _y + 15);
          crc2.arc(_x + 65, _y + 15, 15, 180, 270);
          crc2.moveTo(_x + 75, _y + 25);
          crc2.arc(_x + 75, _y + 25, 15, 180, 270);
          crc2.moveTo(_x + 55, _y + 25);
          crc2.arc(_x + 55, _y + 25, 15, 180, 270);
          crc2.closePath();
          crc2.fill();
          crc2.stroke();

          
          crc2.beginPath();
          crc2.fillStyle = _fillColor;
          crc2.strokeStyle = _strokeColor;
          crc2.moveTo(_x + 80, _y);
          crc2.arc(_x + 80, _y, 15, 180, 270);
          crc2.moveTo(_x + 90, _y);
          crc2.arc(_x + 90, _y - 8, 15, 180, 270);
          crc2.moveTo(_x + 100, _y);
          crc2.arc(_x + 100, _y - 8, 15, 180, 270);
          crc2.moveTo(_x + 110, _y);
          crc2.arc(_x + 110, _y, 15, 180, 270);
          crc2.moveTo(_x + 100, _y);
          crc2.arc(_x + 100, _y + 5, 15, 180, 270);
          crc2.closePath();
          crc2.fill();
          crc2.stroke(); 
    }

    // Busch zeichnen
    function drawBusch1(_x: number, _y: number, _x1: number, _y1: number, r: number, _strokeColor: string, _fillColor: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.arc(_x, _y, 15, 180, 270);
        crc2.moveTo(_x, _y);
        crc2.arc(_x + 10, _y - 10, 15, 180, 270);
        crc2.moveTo(_x, _y);
        crc2.arc(_x + 20, _y - 10, 15, 180, 270);
        crc2.moveTo(_x, _y);
        crc2.arc(_x + 30, _y, 15, 180, 270);
        crc2.moveTo(_x, _y);
        crc2.arc(_x + 20, _y + 5, 15, 180, 270);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
          
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 70, _y);
        crc2.arc(_x + 70, _y, 15, 180, 270);
        crc2.moveTo(_x + 80, _y);
        crc2.arc(_x + 80, _y - 10, 15, 180, 270);
        crc2.moveTo(_x + 90, _y);
        crc2.arc(_x + 90, _y - 10, 15, 180, 270);
        crc2.moveTo(_x + 100, _y);
        crc2.arc(_x + 100, _y, 15, 180, 270);
        crc2.moveTo(_x + 90, _y);
        crc2.arc(_x + 90, _y + 5, 15, 180, 270);
        crc2.closePath();
        crc2.fill();
        crc2.stroke(); 
    }
  

    // Bank zeichnen
    function drawBank(_x: number, _y: number, _strokeColor: string, _fillColor: string, _strokeColor1: string, _fillColor1: string): void {

        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 30, _y - 2);
        crc2.lineTo(_x + 50, _y - 5);
        crc2.lineTo(_x + 50, _y + 93);
        crc2.lineTo(_x + 30, _y + 100);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 120, _y - 14);
        crc2.lineTo(_x + 140, _y - 17);
        crc2.lineTo(_x + 140, _y + 80);
        crc2.lineTo(_x + 120, _y + 85);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
        crc2.beginPath();

        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 120, _y + 40);
        crc2.lineTo(_x + 140, _y + 38);
        crc2.lineTo(_x + 165, _y + 45);
        crc2.lineTo(_x + 145, _y + 45);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();

        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 30, _y + 48);
        crc2.lineTo(_x + 50, _y + 44);
        crc2.lineTo(_x + 75, _y + 60);
        crc2.lineTo(_x + 55, _y + 58);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();

        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 145, _y + 38);
        crc2.lineTo(_x + 160, _y + 45);
        crc2.lineTo(_x + 160, _y + 95);
        crc2.lineTo(_x + 145, _y + 100);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();


        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 55, _y + 63);
        crc2.lineTo(_x + 75, _y + 60);
        crc2.lineTo(_x + 75, _y + 115);
        crc2.lineTo(_x + 55, _y + 120);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();

        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x, _y);
        crc2.lineTo(_x + 160, _y - 20);
        crc2.lineTo(_x + 160, _y - 5);
        crc2.lineTo(_x, _y + 15);
        crc2.closePath();

        crc2.moveTo(_x, _y + 28);
        crc2.lineTo(_x + 160, _y + 8);
        crc2.lineTo(_x + 160, _y + 22);
        crc2.lineTo(_x, _y + 43);
        crc2.closePath();

        crc2.moveTo(_x + 5, _y + 49);
        crc2.lineTo(_x + 160, _y + 28);
        crc2.lineTo(_x + 175, _y + 32);
        crc2.lineTo(_x + 20, _y + 55);
        crc2.closePath();

        crc2.moveTo(_x + 27, _y + 59);
        crc2.lineTo(_x + 184, _y + 37);
        crc2.lineTo(_x + 197, _y + 40);
        crc2.lineTo(_x + 40, _y + 64);
        crc2.closePath();


        crc2.fill();
        crc2.stroke();


    }
    
    // Fluss zeichen
    function drawFluss(_x: number, _y: number, _strokeColor: string, _fillColor: string, _strokeColor1: string, _fillColor1: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 50, _y - 2);
        crc2.lineTo(_x + 10 , _y + 50);
        crc2.lineTo(_x  , _y + 60);
        crc2.lineTo(_x - 8  , _y + 65);
        crc2.lineTo(_x - 20  , _y + 70);
        crc2.lineTo(_x - 30  , _y + 80);
        crc2.lineTo(_x - 40  , _y + 89);
        crc2.lineTo(_x - 50  , _y + 95);
        crc2.lineTo(_x - 60  , _y + 105);
        crc2.lineTo(_x - 110  , _y + 135);
        crc2.lineTo(_x - 200  , _y + 165); 
        crc2.lineTo(_x - 290  , _y + 185); 
        crc2.lineTo(_x - 340  , _y + 195);
        crc2.lineTo(_x - 360  , _y + 210);
        crc2.lineTo(_x - 400  , _y + 230);
        crc2.lineTo(_x - 550  , _y + 200);
        crc2.lineTo(_x - 410  , _y + 140);
        crc2.lineTo(_x - 260  , _y + 110);
        crc2.lineTo(_x - 180  , _y + 90);
        crc2.lineTo(_x - 150  , _y + 80);
        crc2.lineTo(_x - 80  , _y + 50);
        crc2.lineTo(_x - 50  , _y + 30);
        crc2.lineTo(_x + 10, _y );
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
        
        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 45, _y + 3);
        crc2.lineTo(_x + 30 , _y + 3);
        crc2.moveTo(_x - 15 , _y + 15);
        crc2.lineTo(_x + 5 , _y + 15);
        crc2.moveTo(_x + 24 , _y + 30);
        crc2.lineTo(_x - 10 , _y + 30);
        crc2.moveTo(_x - 59 , _y + 40);
        crc2.lineTo(_x - 25 , _y + 45);
        crc2.moveTo(_x - 25 , _y + 70);
        crc2.lineTo(_x - 50 , _y + 68);
        crc2.moveTo(_x - 120 , _y + 70);
        crc2.lineTo(_x - 80 , _y + 80);
        crc2.moveTo(_x - 69 , _y + 110);
        crc2.lineTo(_x - 100 , _y + 104);
        crc2.moveTo(_x - 205 , _y + 100);
        crc2.lineTo(_x - 145 , _y + 115);
        crc2.moveTo(_x - 165 , _y + 150);
        crc2.lineTo(_x - 210 , _y + 140);
        crc2.moveTo(_x - 349 , _y + 130);
        crc2.lineTo(_x - 300 , _y + 150);
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
        
        
    }
    // Bienenkorb Zeichnen
   function drawKorb(_x: number, _y: number, _strokeColor: string, _fillColor: string, _strokeColor1: string, _fillColor1: string): void {
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 125, _y);
        crc2.lineTo(_x + 145, _y);
        crc2.lineTo(_x + 145, _y - 250);
        crc2.lineTo(_x + 125, _y - 250);
        crc2.moveTo(_x + 125, _y -250);
        crc2.lineTo(_x + 80, _y - 250);  
        crc2.lineTo(_x + 80, _y - 400);   
        crc2.lineTo(_x + 185, _y - 400);
        crc2.lineTo(_x + 185, _y - 250);        
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
          
        crc2.beginPath();
        crc2.fillStyle = _fillColor;
        crc2.strokeStyle = _strokeColor;
        crc2.moveTo(_x + 133, _y - 400);
        crc2.arc(_x + 133, _y - 400, 52, 180, 270);
        crc2.moveTo(_x + 80, _y - 380);
        crc2.arc(_x + 80, _y - 380, 20, 180, 270);
        crc2.moveTo(_x + 70, _y - 350);
        crc2.arc(_x + 70, _y - 350, 20, 180, 270);     
        crc2.moveTo(_x + 70, _y - 320);
        crc2.arc(_x + 70, _y - 320, 20, 180, 270); 
        crc2.moveTo(_x + 70, _y - 290);
        crc2.arc(_x + 70, _y - 290, 20, 180, 270);
        crc2.moveTo(_x + 80, _y - 260);
        crc2.arc(_x + 80, _y - 260, 20, 180, 270);    
        crc2.moveTo(_x + 133, _y - 250);
        crc2.arc(_x + 133, _y - 250, 52, 180, 270); 
        crc2.moveTo(_x + 185, _y - 260);
        crc2.arc(_x + 185, _y - 260, 20, 180, 270);
        crc2.moveTo(_x + 195, _y - 290);
        crc2.arc(_x + 195, _y - 290, 20, 180, 270); 
        crc2.moveTo(_x + 195, _y - 320);
        crc2.arc(_x + 195, _y - 320, 20, 180, 270);
        crc2.moveTo(_x + 195, _y - 350);
        crc2.arc(_x + 195, _y - 350, 20, 180, 270); 
        crc2.moveTo(_x + 185, _y - 380);
        crc2.arc(_x + 185, _y - 380, 20, 180, 270);               
        crc2.closePath();
        crc2.fill();
        crc2.stroke();
          
        crc2.beginPath();
        crc2.fillStyle = _fillColor1;
        crc2.strokeStyle = _strokeColor1;
        crc2.moveTo(_x + 120, _y - 250);
        crc2.lineTo(_x + 145, _y - 250); 
        crc2.lineTo(_x + 145, _y - 290);  
        crc2.lineTo(_x + 120, _y - 290);             
        crc2.closePath();
        crc2.fill();
        crc2.stroke();    
    }
    
      
     
    
      // Neue Biene zeichnen
     function drawNeueBiene(): void {
         let b: BieneData = new BieneData();
             b.setStart();
             bee.push(b);
             n++;
        }
    
      // Animate Funktion
         function animate(): void {
             // Hintergrundbild abrufen
            crc2.putImageData(image, 0, 0);   

            for (let i: number = 0; i < n; i++) { 
            let b: BieneData = bee[i];
                // Bewegungsrichtung der Bienen 
                b.move();

               // Wenn Bienen aus dem Bild fliegen
               b.catch();
               b.color = "hsl(" + Math.random() * 360 + ", 100%, 50%)";
                
                // Biene malen lassen
                b.drawBiene();  
            }
             
            window.setTimeout(animate, 20);
        }
    
         }