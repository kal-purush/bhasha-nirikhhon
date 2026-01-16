/*
* @Please read redme.md---;
*/

let symbolBank: Array<string[]> =[                                                                                 
  ["a","a","a","a","a"],
  ["1","1","1","1","1"],
  ["%","%","%","%","%"],
  ["m","m","m","m","m"],
  ["/","/","/","/","/"],
  ["(","(","(","(","("],
];
//*positions* each number in this array represents the current position of each "slot" of the display
let positions: number[] = [0,0,0,0,0];
//*caracterPosition* will represent the current position of the "slot" being changed
let caracterPosition: number = 0;
//*positionToStart will be used to define which display position will start the value change
let positionToStart: number = 0;
//*visor final result
let visor: string[] = ['','','','',''];


function slotCadence(symbols: Array<string[]>): void{
  //represents how many times it has to rotate before finalizing the change in a specific "slot"
  let acumulator: number = getRandomNum(10,25);

  let lineSpinner: number = setInterval(() => {
      //when the accumulator is zero it will only change the next "slots"  
  if(acumulator <= 0 ){
    if(positionToStart >= symbols[0].length){
      clearInterval(lineSpinner);
    }else{
      positionToStart++;
    };
    acumulator = getRandomNum(10,25);
  };

      //this looping is responsible for creating (altering) the entire line of "slots", character by character  
      while(caracterPosition < symbols[0].length){
          //when it reaches the last line of the vector of symbols it will return to the first
          if(positions[caracterPosition] >= symbols.length){
              positions[caracterPosition] = 0;
          }else{
              visor[caracterPosition] = symbols[positions[caracterPosition]][caracterPosition];
              positions[caracterPosition]++;
          };

          //skip to the next "slot"
          caracterPosition++;
  };
      
  //when run again, the "slots" before this one will no longer be changed
  caracterPosition = positionToStart;



  //clean the console and show the temporary result
  console.clear();
  console.log(visor + "...");

      
      acumulator--;		
  },500);
  
};
slotCadence(symbolBank);


//return a random number betwin "min" and "max"
function getRandomNum(min: number, max: number): number {
return  Math.floor(min + ((max - min) * (Math.random())))
};