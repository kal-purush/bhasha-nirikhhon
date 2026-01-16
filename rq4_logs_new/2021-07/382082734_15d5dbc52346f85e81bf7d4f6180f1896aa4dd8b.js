
let input = document.querySelector("input");
let output = document.querySelector(".output");

let chars = [];
let pass = "";

function generate(){
    var num = Number(input.value);
    for(i = 32; i < 126; i++){
        chars.push(String.fromCharCode(i));
    }
    if(num >= 8 && num <= 32){
        for(j = 0; j<num; j++){
            pass += chars[Math.floor(Math.random() * chars.length)];
        }
        output.innerHTML = "Your password :" + pass;
        pass ="";
        input.value ="";

    }else{
        alert("Error! Number Must Be between 8 and 32")
    }
}