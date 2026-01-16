//Pulling off some different properties from Matter
const {Engine, Render, Runner, World, Bodies, Body, Events}= Matter;

const engine=Engine.create();
engine.world.gravity.y=0;
const {world}=engine;
const cellsHorizontal=20;
const cellsVertical=16;
const width=window.innerWidth;
const height=window.innerHeight;
const unitLengthX=width/cellsHorizontal;
const unitLengthY=height/cellsVertical;


const render=Render.create({
    //tell the matter js exactly where we want to show the drawing of the world inside the dom
    element:document.body,
    engine:engine,
    //height amd width of the canvas element
    options:{
        width,
        height,
        wireframes:false,
    }

});
Render.run(render);
Runner.run(Runner.create(),engine);

 
const walls=[
    Bodies.rectangle(width/2,0,width,2,{isStatic:true}),
    Bodies.rectangle(width/2,height,width,2,{isStatic:true}),
    Bodies.rectangle(0,height/2,2,height,{isStatic:true}),
    Bodies.rectangle(width,height/2,2,height,{isStatic:true})
]
World.add(world,walls); //Here we added our shape to the world

//logic to shuffle the array of neighbouring cells of a start cell so that choosing the direction (up/down/left/right)
//where the ball should move becomes random  
const shuffle=(arr)=>{
    let counter=arr.length;
     
     while(counter>0){
       const index=Math.floor(Math.random()*counter);
       counter--;
       const temp=arr[counter];
       arr[counter]=arr[index];
       arr[index]=temp;
     }
     return arr;
}

//Creating 2D grid with initial value as False(unvisited) 
const grid=Array(cellsVertical).fill(null) // will create only one column with 'cellvertical' rows
.map(()=>Array(cellsHorizontal).fill(false)); //will create 'cellsHorizontal' col and 'cell vertical' rows

//create 2D array for vertical mazes and initial value false
const verticals=Array(cellsVertical).fill(null)
.map(()=>Array(cellsHorizontal-1).fill(false));

//create 2D array for horizontal mazes and initial value false -- would help to id
const horizontals=Array(cellsVertical-1).fill(null)
.map(()=>Array(cellsHorizontal).fill(false));


//create random cell number to start with (eg 2,2 )
const startRow=Math.floor(Math.random()*cellsVertical);
const startColumn=Math.floor(Math.random()*cellsHorizontal);

//Main function to create the maze by visiting all cells in the grid
const stepCellThrough=(row,column)=>{
    
//If i have visited the cell at [row,column] then return
if(grid[row][column]){
    
    return;
}
//Mark the cell as visited
grid[row][column]=true;

//Assemble randomly ordered list of neighbours
const neighbours=shuffle([
    [row-1,column,'up'],
    [row,column+1,'right'],
    [row+1,column,'down'],
    [row,column-1,'left']
]);

//For each neighbour..
for(let neighbour of neighbours){
    const [nextRow,nextColumn,direction]=neighbour;

//See if the neighbour is out of bounds
 if(nextRow<0 || nextRow >=cellsVertical || nextColumn <0 || nextColumn >=cellsHorizontal){
     continue;
 }
//If we have visited that neighbour ,continue to next neighbour
if(grid[nextRow][nextColumn]){
    continue;   
}
//Remove a wall from horizontal or vertical
if(direction === 'left'){
    verticals[row][column-1]=true;
}
else if(direction === 'right'){
    verticals[row][column]=true;
}
else if(direction === 'up'){
    horizontals[row-1][column]=true;
}
else if(direction === 'down'){
    horizontals[row][column]=true;
}
//Visit that next cell
stepCellThrough(nextRow,nextColumn);
}


};
stepCellThrough(startRow,startColumn);

//Parses through the horizontal grid and identifies all 'False' cells and turn them into walls
horizontals.forEach((row,rowIndex)=>{
    row.forEach((open,columIndex)=>{
        if(open){
            return;
        }
        const wall=Bodies.rectangle(
            columIndex * unitLengthX + unitLengthX/2,
            rowIndex * unitLengthY + unitLengthY,
            unitLengthX,5,{
                isStatic:true,  //to prevent the element from falling
                label:'walls',  //labelling all the walls for animation effect when ball meets the goal
                render:{
                    fillStyle:'red',
                }
            }
           
        );
        World.add(world,wall);
    })
})

//Parses through the Vertical grid and identifies all 'False' cells and turn them into walls
verticals.forEach((row,rowIndex)=>{
    row.forEach((open,columIndex)=>{
        if(open){   //if any of the cell is true then return
            return;
        }
        const wall=Bodies.rectangle(
            columIndex * unitLengthX + unitLengthX,
            rowIndex * unitLengthY + unitLengthY /2,
            5,unitLengthY,{
                isStatic:true, 
                label:'walls',
                render:{
                    fillStyle:'red',
                }
            }
        );
        World.add(world,wall);
    })
   

})
//creating the goal rectangle in the last cell
const goal=Bodies.rectangle(
    width - unitLengthX/2,
    height - unitLengthY/2,
    unitLengthX * 0.7,  
    unitLengthY * 0.7,{
        isStatic:true,
        label:'goal',
        render:{
            fillStyle:'orange',
        }
        
    }
)
World.add(world,goal);

//Creating ball in the first cell
const ballRadius=Math.min(unitLengthX,unitLengthY);
const ball=Bodies.circle(
    unitLengthX/2,
    unitLengthY/2,
    ballRadius/4,
    {
        label:'ball',
        render:{
            fillStyle:'blue',
        }
    }
)
World.add(world,ball);

//Identifying which key is pressed to move the ball with certain velocity 
document.addEventListener('keydown',(event)=>{
    const {x,y}=ball.velocity;
    
    if(event.keyCode === 38 ){
       Body.setVelocity(ball,{x,y:y-5});  //will move up (w)
      
    }
    if(event.keyCode === 39 ){
        Body.setVelocity(ball,{x:x+5,y});  //will move right (d)
        
    }
    if(event.keyCode === 40 ){
        Body.setVelocity(ball,{x,y:y+5}); //will move down with 5 units velocity (s)
        
    }
    if(event.keyCode === 37 ){
        Body.setVelocity(ball,{x:x-5,y});  //will move left (a)

    }
})

//Win condition
Events.on(engine,'collisionStart',event=>{
    //this event will log each collision of ball with rectangles. Pairs-->Collision-->bodyA,bodyB
    event.pairs.forEach((collision)=>{
//        console.log(collision);
        label=['goal','ball'];
        if(label.includes(collision.bodyA.label) && label.includes(collision.bodyB.label)){
            document.querySelector('.winner').classList.remove('hidden');
            world.gravity.y=1;
            for(let body of world.bodies){
                if(body.label === 'walls'){
                    Body.setStatic(body,false);  //to give an animation effect(all the bodies fall apart downwards) when user wins
                }
            }

        }
    })
})