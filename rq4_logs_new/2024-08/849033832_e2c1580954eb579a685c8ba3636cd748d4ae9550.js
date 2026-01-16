let botao = document.querySelector('.botao')

botao.addEventListener('click', enviar)

function enviar(){
    let nome = document.querySelector('.nome').value
    let numero = document.querySelector('.numero').value

    console.log(nome, numero)
    if(nome === '' || numero === ''){
        alert('Verifique se todos os dados preenchidos')
        preventDefault()
    }
    else{
        alert('Cadastro efetuado com sucesso!')
    }
}