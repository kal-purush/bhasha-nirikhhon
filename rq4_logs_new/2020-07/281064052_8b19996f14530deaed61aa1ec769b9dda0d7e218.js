let data = require('./data.json')

const operators = {
  "add":"+",
  "subtract":"-",
  "multiply":"*",
  "divide":"/",
  "equal":"="
};

const arrayOperators = ["+", "-", "*","/", "="]
const oppositeOperators = {
  "+":"-",
  "-":"+",
  "*":"/",
  "/":"*"
};


// 1st Expression
let firstExpression = parseExpression(data);
console.log(firstExpression.slice(",").join(""));
// 2nd Expression 
let secondExpression = reverseExpression(data)
console.log(secondExpression.slice(",").join(""))
//Result
let solveExpression = doCalculation(secondExpression)
console.log("Answer: "+solveExpression);


//Functions
function getRhs(data){
  let rhsMap = {};
  if (data.op && data.rhs){
    rhsMap[operators[data.op]] = data.rhs
  }
  return rhsMap
}

function getLhs(data) {
  let lhsMap = {};
  if (data.lhs)  {
    if (data.lhs.op && data.lhs.lhs) {
      lhsMap[operators[data.lhs.op]] = data.lhs.lhs
    }
    if (data.lhs.rhs) {
      lhsMap["rhs"] = {
        [operators[data.lhs.rhs.op]] : [data.lhs.rhs.lhs, data.lhs.rhs.rhs]
      }
    }
  }
  return lhsMap
}

function parseExpression (data){
  let array = [];
  let rhs = getRhs(data);
  let lhs = getLhs(data);

  for (let key in lhs) {
    if (arrayOperators.includes(key)) {
      array.push(lhs[key])
      array.push(key)
    }else {
      for (let rhsKey in lhs[key]) {
        
        if (arrayOperators.includes(rhsKey)) {
          for (let i=0 ; i<= lhs[key][rhsKey].length; i++) {
            if (i < lhs[key][rhsKey].length) {
              array.push("(")
              array.push(lhs[key][rhsKey].shift())
              array.push(rhsKey)
            }  
            else {
              array.push(lhs[key][rhsKey].shift())
              array.push(")")
              break;
            }
          }
        }
      }
    }
  }

  for (let key in rhs) {
    if (arrayOperators.includes(key)) {
      array.push(key)
      array.push(rhs[key])
    }
  }

  return array;
}


function reverseExpression(data) {
  let array = [];
  let rhs = getRhs(data);
  let lhs = getLhs(data);

  for (let key in rhs) {
    if (arrayOperators.includes(key)) {
      array.push(key)
      array.push("(")
      array.push(rhs[key])
    }
  }

  for (let key in lhs) {
    if (arrayOperators.includes(key)) {
      array.push(oppositeOperators[key])
      array.push(lhs[key])
      array.push(")")
    }else {
      for (let rhsKey in lhs[key]) {
        if (arrayOperators.includes(rhsKey)) {
          for (let i=0 ; i<= lhs[key][rhsKey].length; i++) {
            if (i < lhs[key][rhsKey].length) {
              array.push(oppositeOperators[rhsKey])
              array.unshift(lhs[key][rhsKey].shift())
            }  
            else {
              array.push(lhs[key][rhsKey].shift())
              break;
            }
          }
        }
      }
    }
  }
  return array;
}

function doCalculation(array) {
  let checkOpearator = ["+", "-", "*","/"]
  let operationArray = array.filter( a => typeof a == 'number' || checkOpearator.includes(a))
  return result(operationArray)
}

function result(operationArray) {
  for (let i =0; i<operationArray.length; i++) {
    let result = doOperation(operationArray.shift(operationArray[i]), operationArray.shift(operationArray[i+1]), operationArray.shift(operationArray[i+2]))
    operationArray.unshift(result);
  }
  return operationArray[operationArray.length-1]
}

function doOperation (operand1, operator, opearand2) {
  if (arrayOperators.includes(operator) && operator == "+") return operand1 + opearand2
  else if (arrayOperators.includes(operator) && operator == "-") return operand1 - opearand2
  else if (arrayOperators.includes(operator) && operator == "*") return operand1 * opearand2
  else if (arrayOperators.includes(operator) && operator == "/") return operand1 / opearand2
}