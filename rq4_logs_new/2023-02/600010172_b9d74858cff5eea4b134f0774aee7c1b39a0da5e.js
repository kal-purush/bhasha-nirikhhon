const fisrtColor =document.querySelector('.color__one');
const secondColor =document.querySelector('.color__two');
const body = document.querySelector('body')
fisrtColor.addEventListener('input',(e)=>{
    const value = e.target.value;
    body.style.background =`linear-gradient(${secondColor.value},${value})`;
})
secondColor.addEventListener('input',(e)=>{
    const value = e.target.value;
    body.style.background =`linear-gradient(${fisrtColor.value},${value})`;
});