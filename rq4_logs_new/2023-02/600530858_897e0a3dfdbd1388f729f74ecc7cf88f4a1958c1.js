// header
let header=document.querySelector('.header'),
    template=document.querySelector('.template');
    headerHeight=header.clientHeight+template.clientHeight,
    scrollOffset=0,
    checkScroll(scrollOffset);
window.addEventListener('scroll',function(){
  let scrollOffset=this.scrollY;
  checkScroll(scrollOffset);
})

function checkScroll(scrollOffset){
  if (scrollOffset>=headerHeight){
    header.classList.add("fixed");
  }
  else{
    header.classList.remove("fixed");
  }
}

// paragraphs
let paragraphs = document.querySelector('.details__paragraphs');
paragraphs.addEventListener('click',function(event){
  let elemId = event.target.dataset.toggleId;
  let arrowId=event.target.dataset.arrow;
  if (!elemId) return;
  if (!arrowId) return;
  let elem = document.getElementById(elemId);
  let arrow = document.getElementById(arrowId);
  elem.style.display = (elem.style.display!='block') ? 'block' : 'none';
  arrow.style.transform = (arrow.style.transform=='rotate(360deg)') ? 'rotate(180deg)' : 'rotate(360deg)';
});

// menu burger
let burger = document.querySelector('.header__burger'),
    menu = document.querySelector('.header__burger-nav');
burger.addEventListener('click',function(){
  event.preventDefault();
  if (!menu.classList.contains('active')){
    menu.classList.add('active');
  }
  else{
    menu.classList.remove('active');
  }
 
});

