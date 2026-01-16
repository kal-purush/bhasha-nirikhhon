var backdrop = document.querySelector('.backdrop');
var modal = document.querySelector('.modal');
var selectPlanButtons = document.querySelectorAll('.plan button');
var toggleButton = document.querySelector('.toggle-button');
var mobileNav = document.querySelector('.mobile-nav');
var modalNo = document.querySelector('.modal__action--negative');

console.log(backdrop);

console.log(selectPlanButtons);

selectPlanButtons.forEach(x => {
   x.addEventListener('click',()=>{
         
         modal.classList.add('open');
         backdrop.classList.add('open');
   });
})

  function closeModel()  {
    modal.classList.remove('open');
         backdrop.classList.remove('open');
    };


backdrop.addEventListener('click',()=>{
   
    mobileNav.classList.remove('open');
    closeModel();
});

modalNo.addEventListener('click',()=>{
    mobileNav.style.display = 'none';
    closeModel();
});

toggleButton.addEventListener('click',()=>{
    mobileNav.classList.add('open');
 backdrop.classList.add('open');
});