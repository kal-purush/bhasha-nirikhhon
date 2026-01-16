const pesoAtual = document.getElementById("#pesoAtual");

const pesoMax = 10 + (2*(document.getElementById("#atributeFOR")));

const alterarPesoAtual = () => { 
    progresso.setAttribute("style", "width: " + pesoAtual.value + "%");
}