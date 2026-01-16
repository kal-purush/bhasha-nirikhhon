//Reverse a string
let string1 = "Hello World!";
let string1Reversed = "";

for(let i = string1.length; i >= 0; i--){
    string1Reversed += string1.charAt(i);
}

console.log(string1Reversed);

//Reverse every word in a string
let string2Reversed = "";

let string1Arr = string1.split(" ");
string1Arr.forEach(word => {
    for(let i = word.length; i >= 0; i--){
        string2Reversed += word.charAt(i);
    }
    string2Reversed += " ";
})

console.log(string2Reversed);