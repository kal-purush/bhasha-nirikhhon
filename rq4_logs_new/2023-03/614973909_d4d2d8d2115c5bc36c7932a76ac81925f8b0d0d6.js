function shine(glow){

    return setInterval(() => {glow.style.textShadow = '0 0 10px rgba(255, 255, 255, 1)'},500);
}

function accumulate(e){

    /*Quando acionado o evento de teclado, os "key" vão entrando 
    nesta função para acumular os valores dentros da variável accumulator. Aqui será detectads os enventos que não forem pertencentes a uma calculadora, como uma letra, por exemplo, como também identificar os comando de operações, para que a variável accumulator retorne vazia novamente quando estes operadores forem chamados */
    
    if(document.querySelector(`button[data-key="${e}"]`)){
        keeping = e;
        if(keeping.lenght === 1){
            accumulator = keeping;
        }
        else{
            accumulator = accumulator + keeping;
        }
        console.log(`valor${accumulator}`)

        document.querySelector(".screenDiv").textContent = accumulator === 0 ? "0" : accumulator;
    }
    else if (document.querySelector(`button[data-keyOperator="${e}"]`) || document.querySelector(`button[data-keyEqual="${e}"]`) ) {

        /* Fila de números e operadores que irão para a função da fila de operação */

        if(e === "+"){
            operateQueue(Number(accumulator));
            operateQueue(e);
        }
        else if (e === "-"){
            operateQueue(Number(accumulator));
            operateQueue(e);
        }
        else if (e === "*"){
            operateQueue(Number(accumulator));
            operateQueue(e);   
        }
        else if (e === "/"){
            operateQueue(Number(accumulator));
            operateQueue(e);
        }
        else if (e ==="%"){
            operateQueue(Number(accumulator));
            operateQueue(e);            
        }
        else if (e === "Enter"){
            operateQueue(Number(accumulator));
            operateQueue(e); 
        }
        accumulator = '';        
    }
    else if (document.querySelector(`button[data-keyFunctions="${e}"]`)){
        if (e === "c"){
            operateQueue(e);
            accumulator = '';
        }
        else if (e === "a"){
            accumulator = "";
            document.querySelector(".screenDiv").textContent = accumulator === 0 ? "0" : accumulator;
        }
        
    }
}

function operateQueue (checkData){

    operationQueue.push(checkData);

     
   if (operationQueue[0] === "+" || operationQueue[0] === "-" || operationQueue[0] === "*" || operationQueue[0] === "/" || operationQueue[0] === "Enter" || operationQueue[0] === "c" || operationQueue[0] === '%'){
        if (operationQueue.length === 2){
            calculate(operationQueue);
            operationQueue = [];
        }
    }
    /* Esta chamada de um array de tamanho 3 são aqueles do começo de uma operação de uma calculadora */

    else if (operationQueue.length === 3){
        calculate(operationQueue);
        operationQueue = [];
    }

}

function calculate(listArray){

    for (let i = 0; i < 3; i++){
        if(listArray[i] === "+" ){
            memorizing = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "-" ){
            memorizing = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "*" ){
            memorizing = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "/" ){
            memorizing = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "%" ){
            memorizing = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "Enter" ){
            equalEnter = (listArray.splice(i,1)); 
        }
        else if (listArray[i] === "c"){
            c = (listArray.splice(i,1));
        }        
    }

    if (memorizing == '+'){
        result += listArray.reduce((total, current) => total + current);
        if(Number.isNaN(result)){
            result = listArray.reduce((total, current) => total + current);
        }        
    }
    else if (memorizing == '-'){
        result -= listArray.reduce((total, current) => total - current);
        if(Number.isNaN(result)){
            result = listArray.reduce((total, current) => total - current);
        }          
    }
    else if (memorizing == '*'){
        result *= listArray.reduce((total, current) => total * current);
        if(Number.isNaN(result)){
            result = listArray.reduce((total, current) => total * current);
        }          
    }
    else if (memorizing == '/'){
        result /= listArray.reduce((total, current) => total / current);
        if(Number.isNaN(result)){
            result = listArray.reduce((total, current) => total / current);
        }          
    }
    else if (memorizing == '%'){
        if (listArray.length === 2){
            result = listArray[0]*(listArray[1]/100);
        }
        else if (listArray.length === 1){
            result = result * (listArray[0]/100);
        }                                
    }
    if(equalEnter == 'Enter'){
        result = Number(listArray);
        equalEnter = '';
    }
    
    if(c == 'c'){
        listArray = [];
        result = listArray;
        memorizing = '';
        equalEnter = '';        
    }
    
    document.querySelector(".screenDiv").textContent = result === 0 || isNaN(result) ? "0" : result;

    console.log(result);
}

const glow = document.querySelector('.glow');

let accumulator = '';
let accumulatorPerc1 = 0;
let accumulatorPerc2 = 0;
let equal = "";
let operationQueue = [];
let listArray = [];
let memorizing = "";
let result;
let equalEnter;
let c;

shine(glow);

document.querySelectorAll('.buttonKeys').forEach(button => button.addEventListener('keydown', e => accumulate(e.key)));