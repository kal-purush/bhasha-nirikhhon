/* Função para alterar o menu fixo */
const ChangeMenuFixo = () => {
    const ChangeMenu = document.getElementById("navbar");

    if(scrollY >= 40){
        ChangeMenu.style.padding = "10px";
    }else{
        ChangeMenu.style.padding = "15px";
    }
}

window.addEventListener("scroll", ChangeMenuFixo);