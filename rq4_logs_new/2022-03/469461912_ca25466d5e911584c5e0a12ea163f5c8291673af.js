

const api = "https://api.api-futebol.com.br/v1/partidas/4849"

let headers = new Headers();

headers.append('Athorization', 'Bearer live_c1c639c0f6b0e2a135e5b62ad74307')
console.log(headers);

const options = {headers: {'Authorization' : 'Bearer live_c1c639c0f6b0e2a135e5b62ad74307'},
}


function buscarinformacao() {
    fetch(api, options)
   .then(resposta => {
     resposta.json()
      .then(obj => {
      mostrarinformacao(obj);
    })
  })
  .catch()
  
  }



function mostrarinformacao(infor){
    console.log(infor,'resultado')

    var camp = document.createElement('h1')
    camp.innerText = infor.campeonato.edicao.nome
    camp.style.marginLeft='160px'

    var data = document.createElement('h2')
    data.innerText = infor.placar
    data.style.marginLeft='50px'

    var hora = document.createElement('h2')
    hora.innerText = infor.rodada

    var image = document.createElement('img')
    image.setAttribute('src',  infor.time_mandante.escudo)

    var image2 = document.createElement('img')
    image2.setAttribute('src',  infor.time_visitante.escudo)

    var datacerta = document.createElement('h2');
    datacerta.innerText= infor.data_realizacao
    datacerta.style.marginLeft='70px'
    datacerta.style.marginTop='-27px'

    var horadojogo = document.createElement('h2')
    horadojogo.innerText= infor.hora_realizacao
    horadojogo.style.marginLeft='70px'
    horadojogo.style.marginTop='-27px'

    var placarcasa = document.createElement('h1')
    placarcasa.innerText=infor.placar_mandante
    placarcasa.style.fontSize='60px'
    placarcasa.style.marginLeft='30px'
    placarcasa.style.marginTop='5px'

    var placardefora = document.createElement('h2')
    placardefora.innerText=infor.placar_visitante
    placardefora.style.fontSize='60px'
    placardefora.style.marginLeft='30px'
    placardefora.style.marginTop='5px'


    var local = document.createElement('h2')
    local.innerText=infor.estadio
    local.style.marginLeft='70px'
    local.style.marginTop='-27px'
   
    document.getElementById('escudofora').append(hora);
    document.getElementById('escudocasa').append(data);
    document.getElementById('nomelogo').append(camp);
    document.getElementById('logocasa').append(image);
    document.getElementById('logofora').append(image2);
    document.getElementById('datacerta').append(datacerta);
    document.getElementById('horajogo').append(horadojogo);
    document.getElementById('placarcasa').append( placarcasa );
    document.getElementById('placardefora').append(placardefora );
    document.getElementById('localjogo').append(local);


}

document.write(buscarinformacao());