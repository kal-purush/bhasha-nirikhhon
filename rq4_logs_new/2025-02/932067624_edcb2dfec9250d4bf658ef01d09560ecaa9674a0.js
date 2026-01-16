// counter app

document.addEventListener("counter",init);

let count = Number(sessionStorage.getItem("count"))|| 0;

function init() {
    document.getElementById("count").textContent=count;
}
function increasecount(){
    count++;
    updatecount();
}
function decreasecount(){
count--;
updatecount();
}
function resetcount(){
    count = 0;
    updatecount();
}
function updatecount(){
    document.getElementById("count").textContent=count;
    sessionStorage.setItem("count",count);
}
