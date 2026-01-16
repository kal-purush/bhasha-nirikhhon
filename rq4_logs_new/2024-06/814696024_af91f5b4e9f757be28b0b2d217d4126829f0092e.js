const cards = document.querySelectorAll(".card");

let check = () =>{
    triggerTop = window.innerHeight /5*4;
    cards.forEach(card => {
        let checkTop = card.getBoundingClientReact().top;
        if(triggerTop > checkTop){
            card.classList.add("display");
        }
        else{
            card.classList.remove("display");
        }
    })
}

check();
window.addEventListener("scroll",check);