        var canvas = document.getElementById("canvas1");   //get the hold of the canvas using its id
		var ctx=canvas.getContext("2d");                   // get the 2D context of the canvas
		
		
	    // function to draw a circle
	    function draw_circle(x,y,i,click){ 
		var radius=30;
		ctx.beginPath();
		ctx.arc(x,y,radius,0,7);
		
		if(click==0)
		   ctx.fillStyle="#000000";  //black
	    else if(click==1)
	       ctx.fillStyle="#8BC34A";  //green
	    else if(click==2)
		   ctx.fillStyle="red";      //red
		else if(click==3) 
		   ctx.fillStyle="blue";     //blue
        else if(click==4)
		   ctx.fillStyle="#4DB6AC";  //light blue
	   else if(click==5)
		   ctx.fillStyle="#FF4081";  //pink
	   else if(click==6)
		   ctx.fillStyle="#EA80FC";  //purple
	   else if(click==7)
		   ctx.fillStyle="#FFA726";  //orange
	   else if(click==8)
		   ctx.fillStyle="#76FF03";  //light green
		
           ctx.fill();
		
		    // to write node number
            ctx.font = '20pt Calibri';
            ctx.fillStyle = 'white';
            ctx.textAlign = 'center';
            ctx.fillText(i, x, y); 
	   }
	   
	    //function to draw edges
	    function draw_line(sx,sy,dx,dy){       // sx->source x-coordinate , dx-> destination x-coordinate
		ctx.beginPath();
		ctx.moveTo(sx,sy);
		ctx.lineTo(dx,dy);
	    ctx.strokeStyle="black";
        ctx.lineWidth=3;
		ctx.stroke();
		   
	   }
	
	   //functioning of the submit box
	    function submit_box()
		{		
		      var j,i,color_state,flag=0;
              once_submit=true;	
			  ctx.lineJoin = "round";
              ctx.lineWidth = cornerRadius;
              ctx.fillStyle="#CCFF90";
              ctx.strokeStyle="#CCFF90";
              ctx.strokeRect(answer.x+(cornerRadius/2), answer.y+(cornerRadius/2), answer.w-cornerRadius, answer.h-cornerRadius);
              ctx.fillRect(answer.x+(cornerRadius/2), answer.y+(cornerRadius/2), answer.w-cornerRadius, answer.h-cornerRadius);
			  ctx.font = '20pt Calibri';
              ctx.fillStyle = 'white';
              ctx.textAlign = 'center';
			  ctx.fillText("Submit",1325,730);
	
		
        // it is used to calculate total no of colors used in the graph		
			for(j=0;j<circle.length;j++)        
			{
				if(which_colors_used[clicks[j]]==false)
				{
					which_colors_used[clicks[j]]=true;
					no_of_colors_used++;
				}
				
			}
			  
			
			//it is used to calculate whether the graph colored is correct or not
			for(j=0;j<circle.length;j++)        
			{                                  
				color_state=clicks[j];
				
				for(i=0;i<adj[j].length;i++)
				{
					if(color_state==clicks[adj[j][i]] || color_state==0 || clicks[adj[j][i]]==0)
					{ 
				       result="incorrect";
				       flag=1;
				       break;
				    }		
				}
				if(flag==1)
					break;				
			}
	            if(flag==0)		
				{
			        result="correct";
			    }

             
			  var team=document.getElementById('team_name').value;
			  
			  var arr={"team":team,"moves":moves,"solution":result,"no_of_colors":no_of_colors_used,"clicks":clicks};
			  $.post("graph_coloring.php",arr,
				function(data)
				{
					alert(team+"\nYour answer is Submitted");
				});		
		}
		
		
	
       //function to get the position of the clicked node
	   function getPosition(event)
      {
        var x = new Number();
        var y = new Number();
		
		
        if (event.x != undefined && event.y != undefined)
        {
			
        var rect = canvas.getBoundingClientRect();    
		var x = event.x - rect.left;
		var y = event.y - rect.top;
	
		  var j,k;
		  var radius=30;
		  var flag=0; 
		    
		  if(x>=answer.x && x<=answer.x+answer.w && y>=answer.y && y<=answer.y+answer.h)
		  {
			  
			   if(once_submit==false){
				  once_submit=true;
			       submit_box();
			  }
		  }
			
		
		  //making sure after submission ,no node is clicked then 
	      if(once_submit==false){
		    for(j=0;j<circle.length;j++)
		  {
			  if((((x-circle[j].x)*(x-circle[j].x))+((y-circle[j].y)*(y-circle[j].y)))<=(radius*radius))
			  {
				  if(j!=last_clicked)        //if the node is not the last clicked node
				  {
					 moves++; 
					 last_clicked=j;
				  }
				  if(clicks[j]==0)  
				  {  clicks[j]=1;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
				  }
                  else if(clicks[j]==1)  
				  {   clicks[j]=2;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
				  }	
                  else if(clicks[j]==2)
				  {   clicks[j]=3;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
				  }	
				  else if(clicks[j]==3)
				  {   clicks[j]=4;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
				  }	
				  else if(clicks[j]==4)
				  {   clicks[j]=5;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
					  
				  }		
				  else if(clicks[j]==5)
				  {   clicks[j]=6;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);				  
			      }
				  				 
				  else if(clicks[j]==6)
				  {   clicks[j]=7;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);	
                  }					  
			     else if(clicks[j]==7)
				  {   clicks[j]=8;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
		          }
				  else if(clicks[j]==8)
				  {   clicks[j]=0;
           			  draw_circle(circle[j].x,circle[j].y,j,clicks[j]);
				  }
            }
		  }
		  
		ctx.fillStyle="#FFD600";           // updating the no of moves
		ctx.fillRect(1200,640,300,50);   
		ctx.fillStyle="black";
		ctx.font = 'bold 25pt Calibri';	
		ctx.fillText("No of moves : "+moves,1320,670);
	    }}
	  }	
	   
	   var circle=
	      [{ x:200,y:200},  //0
		   { x:600,y:500},  //1
		   { x:250,y:550},  //2
		   { x:400,y:150},  //3
		   { x:250,y:350},  //4
		   { x:550,y:300},  //5
		   { x:850,y:580},  //6
		   { x:500,y:650},  //7
           { x:100,y:100},  //8   
           { x:430,y:450},  //9      
		   { x:700,y:250},  //10  
           { x:800,y:100},  //11 
           { x:300,y:700},  //12 
           { x:900,y:350},  //13 
           { x:1000,y:700}, //14  
           { x:100,y:400},  //15
		   { x:150,y:650},	//16
		   { x:980,y:200},  //17
		   { x:1100,y:500},	//18
		   { x:520,y:740}	//19	   
		   ];	
		   
		//to create lines   
	   draw_line(200,200,850,580);  //0->6 
       draw_line(850,580,500,650);  //6->7
	   draw_line(500,650,250,550);  //7->2	  
	   draw_line(250,550,550,300);  //2->5	  
	   draw_line(550,300,100,100);  //5->8	   
	   draw_line(100,100,100,400);  //8->15	   
	   draw_line(100,400,300,700);  //15->12		
       draw_line(430,450,600,500);  //9->1
       draw_line(600,500,900,350);  //1->13	
       draw_line(900,350,1000,700); //13->14
       draw_line(1000,700,700,250); //14->10
       draw_line(700,250,400,150);  //10->3
       draw_line(400,150,800,100);  //3->11
	   draw_line(800,100,250,350);  //11->4  
	   draw_line(700,250,980,200);  //10->17
	   draw_line(400,150,980,200);  //3->17 
	   
	   draw_line(200,200,250,350);  //0->4
	   draw_line(200,200,100,400);  //0->15
	   draw_line(250,350,500,650);  //4->7
       draw_line(500,650,1000,700); //7->14
       draw_line(100,400,430,450);  //15->9
       draw_line(550,300,900,350);  //5->13	
       draw_line(500,650,600,500);  //7->1	   
	   draw_line(300,700,1000,700); //12->14
	   draw_line(800,100,900,350);  //11->13
	   draw_line(700,250,800,100);  //10->11
       draw_line(600,500,550,300);  //1->5	
       draw_line(200,200,100,100);  //0->8
       draw_line(500,650,300,700);  //7->12 
       draw_line(100,400,150,650); 	//15->16
       draw_line(150,650,250,350);  //16->4
       draw_line(150,650,300,700);  //16->12	   
	   draw_line(850,580,1000,700); //6->14
	   draw_line(850,580,900,350);  //6->13
	   draw_line(250,550,300,700);  //2->12
	   draw_line(400,150,200,200);  //3->0 
	   draw_line(900,350,980,200);  //13->17 
       draw_line(100,100,800,100);  //8->11	
       draw_line(600,500,850,580); 	//1->6
       draw_line(600,500,980,200);  //1->17
       draw_line(1000,700,980,200); //14->17
       draw_line(800,100,1100,500); //11->18	
       draw_line(980,200,1100,500);	//17->18   
	   draw_line(100,100,400,150);  //8->3
	   draw_line(600,500,1100,500); //1->18 
	   draw_line(250,350,550,300);  //4->5
	   draw_line(250,550,100,400);  //2->15
	   draw_line(250,350,400,150);  //4->3
	   draw_line(550,300,700,250);  //5->10
	   draw_line(300,700,430,450);  //12->9
	   draw_line(500,650,520,740);  //7->19
	   draw_line(850,580,520,740); //6->19
	   draw_line(300,700,520,740); //12->19
	   
	   
	   
	   var i;
	   var once_submit=false;
	   var clicks=[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
	   var moves=0;
	   var last_clicked=-1;
	   var result;
	   var which_colors_used=[true,false,false,false,false,false,false,false,false];
	   var no_of_colors_used=0;
	   
	   //to draw circles
	   for(i=0;i<circle.length;i++)
	   { 
            draw_circle(circle[i].x,circle[i].y,i,0);
	   } 
	   
	   //to create adjacency list 
	     var adj=[];
		 
       	   adj[0]=[3,4,6,8,15];
		   adj[1]=[5,6,7,9,13,17,18];
		   adj[2]=[5,7,12,15];
		   adj[3]=[0,4,8,10,11,17];
		   adj[4]=[0,3,5,7,11,16];
		   adj[5]=[1,2,4,8,10,13];
		   adj[6]=[0,1,7,13,14,19];
		   adj[7]=[1,2,4,6,12,14,19];
		   adj[8]=[0,3,5,11,15];
		   adj[9]=[1,12,15];
		   adj[10]=[3,5,11,14,17];
		   adj[11]=[3,4,8,10,13,18];
		   adj[12]=[2,7,9,14,15,16,19];
		   adj[13]=[1,5,6,11,14,17];
		   adj[14]=[6,7,10,12,13,17];
		   adj[15]=[0,2,8,9,12,16];
		   adj[16]=[4,12,15];
		   adj[17]=[1,3,10,13,14,18];
		   adj[18]=[1,11,17];
		   adj[19]=[7,12,6];
	   	
		
	   // to draw buttons
	   {
		   
		var answer={x:1250,y:700,w:160,h:50};
        var cornerRadius=20;
		
		
       // Set faux rounded corners
       ctx.lineJoin = "round";
       ctx.lineWidth = cornerRadius;
       ctx.fillStyle="rgba(0,200,0,1)";
       ctx.strokeStyle="rgba(0,200,0,1)";

	   ctx.strokeRect(answer.x+(cornerRadius/2), answer.y+(cornerRadius/2), answer.w-cornerRadius, answer.h-cornerRadius);
       ctx.fillRect(answer.x+(cornerRadius/2), answer.y+(cornerRadius/2), answer.w-cornerRadius, answer.h-cornerRadius);
		 
	  ctx.font = '20pt Calibri';
      ctx.fillStyle = 'white';
      ctx.textAlign = 'center';
      ctx.fillText("Submit",1325,730);
     }
		
		
		// to draw colors boxes
		{
			
		ctx.fillStyle="#8BC34A"; //green  
		ctx.fillRect(1300,220,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("1 click",1385,245);	

		ctx.fillStyle="red";  
		ctx.fillRect(1300,270,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("2 clicks",1390,295);	

		ctx.fillStyle="blue";  
		ctx.fillRect(1300,320,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("3 clicks",1390,345);

		ctx.fillStyle="#4DB6AC";  //light blue
		ctx.fillRect(1300,370,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("4 clicks",1390,395);		

		ctx.fillStyle="#FF4081"; //pink  
		ctx.fillRect(1300,420,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("5 clicks",1390,445);
		
		ctx.fillStyle="#EA80FC";  //purple
		ctx.fillRect(1300,470,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("6 clicks",1390,495);

		ctx.fillStyle="#FFA726";  //orange
		ctx.fillRect(1300,520,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("7 clicks",1390,545);
		
        ctx.fillStyle="#76FF03";  //light green
		ctx.fillRect(1300,570,30,30);
	    ctx.fillStyle="black";
	    ctx.font = 'bold 20pt Calibri';
        ctx.fillText("8 clicks",1390,595);
		
		
		ctx.fillStyle="black";
		ctx.font = 'bold 25pt Calibri';	
		ctx.fillText("No of moves : "+moves,1320,670);			
		}
		
       // event listener on mouse click
       canvas.addEventListener("mousedown", getPosition, false);
	 

     // timer 
     function countdown(minutes) {
     seconds=60;		
     mins= minutes;
     function tick() {

        var current_minutes = mins-1;
        seconds--;
		
		ctx.fillStyle="#FFD600";  
		ctx.fillRect(1050,50,600,100); 
		
		var string = current_minutes.toString() + " mins " + (seconds < 10 ? "0" : "") + String(seconds)+" secs";
		ctx.fillStyle="black";
	    ctx.font = 'italic 30pt Calibri';
		ctx.fillText("Time Left : "+string,1280,80);
		
		
        if( seconds > 0 ) {
            setTimeout(tick, 1000);
        } else {
            if(mins > 1){
                countdown(mins-1);           
            }
			else 
			{ 
		      submit_box();
			} 
        }

      }
       tick();
     }

     //to start the time when page is load  
	 countdown(45);		
	 