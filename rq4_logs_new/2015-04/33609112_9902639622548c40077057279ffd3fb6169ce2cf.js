window.onload = function(){
    //Accordion Logic
    var open = false,
        i = 0,
        openContent,
        accordionElem = document.querySelectorAll('div.accordion');
    function accordion(){
        open = !open;
        if(openContent != this && openContent != undefined){
          //console.log('hit first check', openContent, this);
            openContent.style.display = 'none';
            open = true;
        }
        if(openContent == this.querySelector('div.content')){
            open = false;
        }
        if(open){
            openContent = this.querySelector('div.content');
            this.querySelector('div.content').style.display = "block";
        }else{
            this.querySelector('div.content').style.display = "none";
        }
    }

    for(;i < accordionElem.length; i++){
        accordionElem[i].addEventListener('click', accordion);
    }
    //End Accordion Logic
}