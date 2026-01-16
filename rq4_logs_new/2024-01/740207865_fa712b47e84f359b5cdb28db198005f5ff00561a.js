let rquote=document.querySelector(".quote");
let author=document.querySelector(".name");
let button=document.querySelector("#btn");
let api_url="https://api.quotable.io/random";

async function getquote(url){
    btn.innerText="Loading Quote...";
    btn.classList.add("loading");
    const response=await fetch(url);
    var data =await response.json();
    rquote.innerHTML=data.content;
    author.innerHTML=data.author;
    btn.innerText="New Quote";
    btn.classList.remove("loading");
}
getquote(api_url);

function tweet(){
    window.open("https://twitter.com/intent/tweet?text=" +rquote.innerHTML,"Tweet Window","width=600 ,height=300");
}
function sound(){
    let ut= new SpeechSynthesisUtterance(`${rquote.innerText} by ${author.innerText}`);
    speechSynthesis.speak(ut);
}
function copy(){
    navigator.clipboard.writeText(rquote.innerHTML);
}