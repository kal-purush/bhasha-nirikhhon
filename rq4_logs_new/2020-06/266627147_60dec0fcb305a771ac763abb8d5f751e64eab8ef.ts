//remove exclimation mark for demo
const button1 = document.querySelector("button")!;
//ts would give an error here that button might be null to solve it temporarily
//we add an ! in above line to tell ts that a button will exist
button1.addEventListener("click", () => {
  console.log("clicked!!!");
});

//but how does ts know that document will exist and it has a method querySelector()
//we can argue that vanilla js knows this
//but we can write nodejs code in ts and document doesnt exist there
//ts knows about it by the lib option in tsconfig
//it is commented by default
//lets uncomment it and leave it blank we will notice ts no longer knows a lot of stuff now like document

//--------------------strict type checking options------------------
//---------------------noimplicitAny----------------------------
console.log("");
console.log("---------------------noimplicitAny----------------------------");
//set noimplicitAny to true to demo

//the below function data parameter  imlicitly has any type
//and will give an error
function sendAnalytics(data) {
  console.log(data);
}
sendAnalytics("analytics!!!!!!");

//---------------------noimplicitAny Ends----------------------------

//---------------------strictNullChecks----------------------------
console.log("");
console.log(
  "---------------------strictNullChecks----------------------------"
);
//set strictNullChecks to true for demo

//ts would give an error here that button2 might be null if no button exists in html file
//but if we set strictNullChecks to false it will stop complaining
//but the issue will remain and an exception might occur

//uncomment to demo
//const button2 = document.querySelector("button");
/* button2.addEventListener("click", () => {
  console.log("clicked!!!");
}); */

// if  as a developer we are absolutely sure that the button exists
// we can use the ! mark in decleration
const button3 = document.querySelector("button")!;
//ts would not complain even if strictNullChecks is true  as we are explicitly telling
//ts thatbutton 3 will exist
button3.addEventListener("click", () => {
  console.log("clicked!!!");
});

//another way here is to wrap the usage in an if check
const button4 = document.querySelector("Nonexistingbutton");
//ts would not complain even if strictNullChecks is true
// as the if check would avoid null reference
console.log("button 4 is " + button4);
if (button4) {
  button4.addEventListener("click", () => {
    console.log("clicked!!!");
  });
}
//---------------------strictNullChecks Ends----------------------------

//---------------------strictBindCallApply----------------------------
console.log("");
console.log(
  "---------------------strictBindCallApply----------------------------"
);

//set strictBindCallApply to true to demo

const button5 = document.querySelector("button");
function clickHandler(message: string, num1: number) {
  console.log("user clicked " + message);
}
if (button4) {
  //below code will give an error as addEventListener expects a function as second
  //argument and apply returns void in this case as clickHandler retruns void
  //but error will only be reported if  strictBindCallApply is true

  button4.addEventListener("click", clickHandler.apply(null, ["hiiii", 22]));
  //following code should give an error as arguments expected are string number
  //but passed are number,number
  button4.addEventListener("click", clickHandler.bind(null, 33, 22));
  //following code should give an error as arguments expected are string number
  //but passed are number,number as well as call returns void in this case
  // as clickHandler retruns void but expectation of addEventListener is a function
  button4.addEventListener("click", clickHandler.call(null, 33, 22));
}
//---------------------strictBindCallApply Ends----------------------------

//---------------------Code quality options----------------------------

console.log("");
console.log(
  "---------------------Code quality options----------------------------"
);

//--------------------------noUnusedLocals---------------
console.log("");
console.log("--------------------------noUnusedLocals---------------");
//set noUnusedLocals to true to demo this
function demoNoUnusedLocals() {
  //below is an unused local variable
  //ts will give an error on compilation if noUnusedLocals is true
  let abc;
  console.log("noUnusedLocals allowed");
}

//--------------------------noUnusedLocals Ends---------------

//--------------------------noUnusedParameters---------------
console.log("");
console.log("--------------------------noUnusedParameters---------------");
//set noUnusedParameters to true to demo this
function demoNoUnusedParameters(name: string, age: number) {
  //only name parameter is used
  //age parameter is not used
  //if noUnusedParameters is set to true it will give an error
  console.log(name);
}

//--------------------------noUnusedParameters Ends---------------

//--------------------------noImplicitReturns---------------
console.log("");
console.log("--------------------------noImplicitReturns---------------");
//set noImplicitReturns to true to demo this
function demoNoImplicitReturns(num1: number, num2: number) {
  if (num1 + num2 > 0) {
    return num1 + num2;
  } else if (num1 + num2 == 0) {
    //this branch doesnt return anything
    //if noImplicitReturnsis set to true i will give an error
    console.log("correct your input");
  } else {
    //this is allowed
    return;
  }
}

//--------------------------noImplicitReturns Ends ---------------
console.log("");
console.log(
  "--------------------------noFallthroughCasesInSwitch---------------"
);
//set noFallthroughCasesInSwitch to true to demo this
function demoNoFallthroughCasesInSwitch(num1: number) {
  switch (num1) {
    case 1:
      console.log("received 1");
      break;
    //below is allowed
    case 2:
    case 3:
      console.log("received 2 or 3");
      break;
    //below not allowed as below we have a missing break
    // if noFallthroughCasesInSwitch is true ts will give an error
    case 4:
      console.log("received 4");
    case 5:
      console.log("received 5");
  }
}
//--------------------------noFallthroughCasesInSwitch Ends ---------------