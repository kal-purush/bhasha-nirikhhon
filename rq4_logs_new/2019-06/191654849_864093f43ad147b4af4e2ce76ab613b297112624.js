window.onscroll = () => {scroll()};

function scroll(){
    // posição no top da janela
    var scroll = document.body.scrollTop || document.documentElement.scrollTop;
    // altura do documento
    var documento = document.documentElement.scrollHeight;
    // altura da tela
    var janela = document.documentElement.clientHeight;
    // posição final do cursor
    var final = documento - janela;

    // Elemento que quero disparar
    var anima = $(".anima");
    // Metade da altura
    var half  = Math.floor(janela / 2);

    $.each(anima, function(i,v){
        if($(this).offset().top < scroll + half) {
            $(this).addClass('play');
            console.log($(this).offset().top);
        }
    })

    document.getElementById("label").innerHTML = scroll + " " + documento + " " + janela + " " + final;
}