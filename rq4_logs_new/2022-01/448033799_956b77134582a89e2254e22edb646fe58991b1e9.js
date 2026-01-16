class Buttons{

    constructor(){

        //to create the buttons
        this.start = createButton('START');
        this.start.position(displayWidth/2 - 50, displayHeight/2 + 470);
        this.start.style('background', 'lightblue');
        this.start.style('font-size', '30px')

        this.next = createButton('NEXT');
        this.next.position(displayWidth/2 + 70, displayHeight/2+470);
        this.next.hide();
        this.next.style('background', 'lightblue');
        this.next.style('font-size', '30px')

        this.next2 = createButton('NEXT');
        this.next2.position(displayWidth/2 + 100, displayHeight/2 + 470);
        this.next2.hide();
        this.next2.style('background', 'lightblue');
        this.next2.style('font-size', '30px');

        this.next3 = createButton('NEXT');
        this.next3.position(displayWidth/2 + 100, displayHeight/2 + 470);
        this.next3.hide();
        this.next3.style('background', 'lightblue');
        this.next3.style('font-size', '30px');
        
        this.next4 = createButton('NEXT');
        this.next4.position(displayWidth/2 + 100, displayHeight/2 + 470);
        this.next4.hide();
        this.next4.style('background', 'lightblue');
        this.next4.style('font-size', '30px');

        this.next5 = createButton('NEXT');
        this.next5.position(displayWidth/2 + 100, displayHeight/2 + 470);
        this.next5.hide();
        this.next5.style('background', 'lightblue');
        this.next5.style('font-size', '30px');



    }

    display(){

        //to give a command of what will happen if the button is pressed

            this.start.mousePressed(()=>{

            this.next.show();
            text("MILKY WAY GALAXY", 100, 200);
            galaxy.visible = true;
            galaxy.velocityX = 5;
            bgMusic.loop();

           this.start.position(displayWidth/2 - 50, displayHeight/2 + 470);
           
           gameState = 1;

        });

        this.next.mousePressed(()=>{

            this.start.hide();
            this.next.position(displayWidth/2 - 50, displayHeight/2 + 470);

           solarSystem.visible = true;
           solarSystem.velocityX = -5;
            
            
        });
    
       if(solarSystem.x === test.x && solarSystem.y === test.y){

            this.next2.show();
            this.next.hide();
            this.next2.position(displayWidth/2 - 50, displayHeight/2 + 470);

            galaxy.visible = false;
           
            
            this.next2.mousePressed(()=>{

            stars.velocityY = 5;
            stars.visible = true;
                
            })
        }

        if(stars.x === test.x && stars.y === test.y){

            this.next3.show();
            this.next2.hide();
            this.next3.position(displayWidth/2 - 50, displayHeight/2 + 470);


            solarSystem.visible = false;
            
            
            this.next3.mousePressed(()=>{

                blackHole.visible = true;
                blackHole.velocityY = -5;

            })
            
        }

        if(blackHole.x === test.x && blackHole.y === test.y){

            this.next4.show();
            this.next3.hide();
            this.next4.position(displayWidth/2 - 50, displayHeight/2 + 470);

            stars.visible = false;
          

            this.next4.mousePressed(()=>{

                astronomyHistory.visible = true;
                astronomyHistory.velocityX = -5;
                
            })

        }
        
        if(astronomyHistory.x === test.x && astronomyHistory.y === test.y){

            this.next5.show();
            this.next4.hide();
            this.next5.position(displayWidth/2 - 50, displayHeight/2 + 470);

            blackHole.visible = false;

            this.next5.mousePressed(()=>{

                coding.visible = true;
                coding.velocityX = 5;

            })
        }

        if(coding.x === test.x && coding.y === test.y){

            this.next5.hide();
            astronomyHistory.visible = false;
            
       }

   

        
    }
}
