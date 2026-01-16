
// Integrando tooltips, alertas, usando el toggle, etc.


$(function () {
    $('[data-toggle="tooltip"]').tooltip();

    $('#quienesomos').click(function(){
        alert("Empresa Lider en Turismo Nacional")
    });

    $('#destacados').click(function(){
        alert("Recorre Chile!")
    });
    
    $('#contactos').click(function(){
        alert("Hablemos!")
    });
    
    $('.titulo1').click(function(){
        $('.texto1').toggle();
    });
    $('.titulo2').click(function(){
        $('.texto2').toggle();
    });
    $('.titulo3').click(function(){
        $('.texto3').toggle();
    });
    
    $(".ocultar-mostrar").click(function(){
        $(".ocultar-mostrar").slideToggle(1500); 
        $(".ocultar-mostrar").toggle('show');
    });
    $(".ocultar-mostrar2").click(function(){
        $(".ocultar-mostrar2").slideToggle(1500); 
        $(".ocultar-mostrar2").toggle('show');
    });
    $(".ocultar-mostrar3").click(function(){
        $(".ocultar-mostrar3").slideToggle(1500); 
        $(".ocultar-mostrar3").toggle('show');
    });
    
    $('.titulo4').click(function(){
        $('.texto4').toggle();
    })
  })

  // Scrolleo suave solicitado.
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        $('html,body').animate({
            'scrollTop': $($(this).attr('href')).offset().top+'px'
        }, 5000);
        

        document.querySelector(this.getAttribute('href')).scrollIntoView({
            behavior: 'smooth'
        });
    });
});


$("html, body").animate({
    scrollTop: 290
}, 2000);



  $("#quienesomos").hover(function() {
    $(this).css('cursor','pointer').attr('title', 'Vive una gran experiencia junto a nosotros');
}, function() {
    $(this).css('cursor','auto');
});