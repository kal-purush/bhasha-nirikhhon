let acordeon = document.getElementById("acordeon-content"),
    acordeonBody = [...document.querySelectorAll(".acordeon__body")];



function openMenu(element){
   let parent  = element.target.parentNode,
          lastChild = parent.lastElementChild,
          menu = lastChild.firstElementChild;
            
        acordeonBody.map(el => el.style.height = 0);
        
  if(lastChild.clientHeight){
           lastChild.style.height = 0;
   }else{
     let altoMenu = menu.clientHeight;
        lastChild.style.height = `${altoMenu}px`;
    }

}



function getTarget(e){
    
  if(e.target.tagName === "DIV"){
      openMenu(e); 
  }
  
}


acordeon.addEventListener("click", getTarget);