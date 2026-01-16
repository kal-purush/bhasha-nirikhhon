function carregar(){

var msg = window.document.getElementById('msg')
var img = window.document.getElementById('imagem')
var data = new Date()
var hora = data.getHours()
msg.innerHTML = `<strong>Agora são ${hora} Horas.</strong>`

if (hora >= 0 && hora < 12){
    //bom dia
    img.src ='manha.png'   
    document.body.style.background = '#ddae80'
} else if (hora >= 12 && hora < 18){
    //boa tarde
    img.src ='tarde.png'
    document.body.style.background = '#bb520e'
} else{
    //boa noite
    img.src ='noite.png'
    document.body.style.background = '#14272e'
    }
}