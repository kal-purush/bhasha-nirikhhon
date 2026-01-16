//Displays entered input
function display(val){
    document.getElementById("display").value+=val;
}

//Clears value
function clearfun(){
    document.getElementById("display").value='';
}

//Evaluates the expression
function compute(e){
    let expression=document.getElementById("display").value;
    let computedvalue=eval(expression);
    document.getElementById("display").value=computedvalue?computedvalue:'';
    e?.preventDefault()
    
}
//To handle keyboard input
addEventListener("keypress",function(event){
    console.log(event)
    if(event.key=='Enter'){
       
        compute(event);
        event.preventDefault;
    }
    if(event.key.match(/^[0-9-+*/]$/)){
        document.getElementById("display").value+=event.key;

    }
})