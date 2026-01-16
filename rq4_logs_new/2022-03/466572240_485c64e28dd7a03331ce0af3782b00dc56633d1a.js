let newElement = document.getElementsByClassName("filterBtns");
let filterBtns = Array.from(newElement);
let elementArr = document.getElementsByClassName("filterItem");
let btnContent = Array.from(elementArr)

filterBtns.forEach(el => {
//  console.log(el)
  el.addEventListener('click', function(){
    // console.log(this);
    filterBtns.forEach(el => el.classList.remove('active'));
    this.classList.add("active");
    btnContent.forEach(el => el.classList.remove('show'));
    let y = document.querySelector(this.dataset.target);
    // console.log(y.classList)
    y.classList.add("show");
  })
})