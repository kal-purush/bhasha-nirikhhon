// function <<nome da funçao>> (){
//conteudo da função
//}

function login(){
    var email = document.getElementById("frmemail").value //criou uma variavel chamada email e atribuiu o valor presente no id "frmemail"
    var senha = document.getElementById("frmsenha").value
    console.log(email)
    console.log(senha)
    if(email == "login" && senha == "12345"){
        alert("Login realizado com sucesso")
    }
    else{
        alert("Login inválido")
    }
}

function somar(){
    var n1 = parseInt(prompt("Digite o primeiro número"))
    var n2 = parseInt(prompt("Digite o segundo número"))
    //console.log(typeof(n1))  mostra o tipo de variavel
    console.log(typeof(n1))
    console.log(typeof(n2))
    var resultado = n1+n2
    console.log(resultado)
    alert(resultado)
}

function subtrair(){
    var n1 = parseInt(prompt("Digite o primeiro número"))
    var n2 = parseInt(prompt("Digite o segundo número"))
    console.log(typeof(n1))
    console.log(typeof(n2))
    var resultado = n1-n2
    console.log(resultado)
    alert(resultado)
}

function multiplicar(){
    var n1 = parseInt(prompt("Digite o primeiro número"))
    var n2 = parseInt(prompt("Digite o segundo número"))
    console.log(typeof(n1))
    console.log(typeof(n2))
    var resultado = n1*n2
    console.log(resultado)
    alert(resultado)
}

function dividir(){
    var n1 = parseInt(prompt("Digite o primeiro número"))
    var n2 = parseInt(prompt("Digite o segundo número"))
    console.log(typeof(n1))
    console.log(typeof(n2))
    if(n1==0 || n2==0){
        alert("Os valores não podem ser iguais a 0")
    }
    else{
        var resultado = n1/n2
        console.log(resultado)
        alert(resultado)
    }
}

function elevarquad(){
    var n1 = parseInt(prompt("Digite o número"))
    console.log(typeof(n1))
    var resultado = n1*n1
    console.log(resultado)
    alert(resultado)
}

function elevar(){
    var n1 = parseInt(prompt("Digite o primeiro número"))
    var n2 = parseInt(prompt("Digite o segundo número"))
    var resultado = n1**n2
    console.log(resultado)
    alert(resultado)
}

function raiz(){
    var n1 = parseInt(prompt("Digite o número"))
    var resultado =
    console.log(resultado)
    alert(resultado)
}