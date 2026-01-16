//Calculator functions
const screen = document.getElementsByClassName("screen")[0];
let screenNum = screen.innerHTML;
let resultArr = [];
let result=0;

const delKey = document.getElementById("del");
delKey.addEventListener('click', () => {

    if(screenNum != 0) {
        let temp = screenNum.toString();
        console.log(temp)
        if(temp.length == 1){
            screenNum = 0;
        }
        //if no commas or .'s
        else if(( !temp.includes('.')) && ( !temp.includes(','))) {
            screenNum = splitJoin(temp);
        } else if (temp.includes('.')){
            screenNum = splitJoin(temp);
        } else {
            temp = splitJoin(temp);
            temp = temp.toString().replace(/\,/g,'');
            screenNum = parseInt(temp).toLocaleString('en-US');
            console.log(temp)
        }
    } 
    screen.innerHTML = screenNum;
});

function splitJoin(temp) {
    temp = temp.split('');
    temp.pop();
    return temp.join('');
}

const resetKey = document.getElementById('calc-button-reset');
resetKey.addEventListener('click', () => {
    resultArr = [];
    result = '';
    screenNum = 0;
    screen.innerHTML = screenNum;
});

const keys = document.getElementsByClassName("number");
for(let i =0; i <keys.length; i++){
    keys[i].addEventListener('click', changeScreenNum);
}

function changeScreenNum(ev) {
    const keyNum = ev.target.innerHTML;
    
    if(screenNum == 0) {
        screenNum = keyNum;
    } //no commas beyond decimal point, so add keyNum to end
    else if(((keyNum == '.') || (screenNum.toString().includes('.')) 
        || (( !screenNum.toString().includes('.')) && (screenNum.length < 3)))){
        screenNum = screenNum+keyNum;
        // add commas to integers
    } else {
        let temp = screenNum.toString();
            temp = temp.replace(/\,/g,'');
            temp += keyNum;
            screenNum = parseInt(temp).toLocaleString('en-US');
        }
    
    screen.innerHTML = screenNum;
}


const operators = document.getElementsByClassName("operator");
for(let i =0; i <operators.length; i++){
    operators[i].addEventListener('click', addOperator);
}

function addOperator(ev){
    let operator = ev.target.innerHTML;
    if(operator == 'x') {
        operator = '*';
    }
    resultArr.push(screenNum);
    resultArr.push(operator);
    
    screenNum = 0;
}

const equals = document.getElementById('equals');
equals.addEventListener('click', sum);

function sum(ev){
    resultArr.push(screenNum);
    result = resultArr.join('');
    //evaluate string 
    screenNum = window.eval(result);
    //set solution in the screen window
    screen.innerHTML = screenNum;
    //reset
    resultArr = [];
    result = '';
}