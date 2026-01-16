// function toggletext() {
//     let text = document.getElementsByClassName("btn");
//     if (text.style.display === "none") {
//       text.style.display = "block";
//     } else {
//       text.style.display = "none";
//     }
//   }


let btn = document.getElementsByClassName("btn");
let cleanser = document.getElementById("cleanser");
buttonClick.onclick = function(){
    if(cleanser.style.display == "block") {
        (cleanser.style.display = "none")
    }
    else{
        cleanser.style.display = "block";
    }

}