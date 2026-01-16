$(document).ready(function(){
    // Selecionando elementos
    console.log(document.querySelector('header button'));
    console.log($('#cancel'));
    //  Manipulando elementos
    $('header button').click(function(){
       $('form').slideDown();
    })

    $('#cancel').click(function(){
        $('form').slideUp();
    })
    $('form').on('submit', function(e){
        e.preventDefault();
        const enderecoDaNovaImagem = $('#endereco-imagem-nova').val();
        const novoItem = $('<li style="display:none"></li>')
        $(`<img src="${enderecoDaNovaImagem}"/>`).appendTo(novoItem);
        $(`
            <div class="overlay-imagem-link">
                <a href="${enderecoDaNovaImagem}" target="_blank" title="Ver Imagem de tamanho real">Ver Imagem de tamanho real
                </a>
            </div>`
        ).appendTo(novoItem);
        $(novoItem).appendTo('ul');
        $(novoItem).fadeIn(3000);
        $('#endereco-imagem-nova').val('');
    })
})