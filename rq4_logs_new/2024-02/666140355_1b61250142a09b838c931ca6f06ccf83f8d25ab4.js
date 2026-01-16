const scrollers = document.querySelectorAll('.scroller');
let currentArrow= 1;

if(!window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
    addAnimation();
}

document.querySelector(".pop-up-input").addEventListener('keydown', (event) => {
    if(event.key === 'Enter') {
        popUp();
    }
});
document.querySelector(".inquiry-button").addEventListener('keydown', (event) => {
    if(event.key === 'Enter') {
        popUp();
    }
});

function addAnimation() {
    scrollers.forEach(scroller => {
        scroller.setAttribute("data-animated", true);

        const scrollerInner = scroller.querySelector('.scroller__inner');
        const scrollerContent= Array.from(scrollerInner.children);
        
        scrollerContent.forEach(item => {
            const duplicatedItem = item.cloneNode(true);
            duplicatedItem.setAttribute('aria-hidden', true);
            scrollerInner.appendChild(duplicatedItem);
        });

    });
}

function scrollFunction(target) {;
    const element = document.querySelector(`.${target}`);
    element.scrollIntoView({ behavior: 'smooth'});
}

let currentPopUp= 0;
function popUp() {
    const popUpElement= document.querySelector(".pop-up");
    const bodyElement = document.querySelector(".mask-back");
    const message = document.querySelector(".inquiry-button").value;
    if(message== '') {

    }
    else if(currentPopUp==0) {
        popUpElement.style.display="flex";
        document.querySelector(".message-js").value= message;
        message= '';
        bodyElement.style.mask= "linear-gradient(0deg, transparent, white 80%, white 10%, transparent)";
        document.body.style.overflow = "hidden";
        currentPopUp=1;
    } else {
        if(document.querySelector(".pop-up-input-js").value == '') {
            
        }
        else {
            popUpElement.style.display="none";
            bodyElement.style.mask= "unset";
            document.body.style.overflow = "auto";
            currentPopUp=0;
        }
    }
}

function goRight() {
    document.querySelector(".how-we-operate-center").scrollLeft += 240;
    if(currentArrow==3) {
        //nothing
    }
    else if(currentArrow==2) {
        const rigthArrow= document.querySelector(".right");
        rigthArrow.style.backgroundColor= "#101010";
        rigthArrow.style.color= "white";
        currentArrow++;
    }
    else {
        const leftArrow= document.querySelector(".left");
        leftArrow.style.backgroundColor= "white";
        leftArrow.style.color= "#101010";
        currentArrow++;
    }
}
function goLeft() {
    document.querySelector(".how-we-operate-center").scrollLeft -= 240;
    if(currentArrow==1) {
        //nothing
    }
    else if(currentArrow==2) {
        const leftArrow= document.querySelector(".left");
        leftArrow.style.backgroundColor= "#101010";
        leftArrow.style.color= "white";
        currentArrow--;
    }
    else {
        const rigthArrow= document.querySelector(".right");
        rigthArrow.style.backgroundColor= "white";
        rigthArrow.style.color= "#101010";
        currentArrow--;
    }
}