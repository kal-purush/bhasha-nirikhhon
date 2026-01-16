const boton = document.getElementById("btn");
const spinner = document.getElementById("spin");

boton.addEventListener('click',() =>{
    boton.style.display = 'none';

    spinner.style.display = 'inline';
})