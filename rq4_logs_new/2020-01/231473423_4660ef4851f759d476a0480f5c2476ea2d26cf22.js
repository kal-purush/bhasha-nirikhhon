(function() {
    var menuResponsivo = document.getElementById("menuResponsivo");
    var btnResponsivo = document.getElementById("btnResponsivo");
  
    btnResponsivo.addEventListener("click", function(e) {
      e.preventDefault();
      document.body.scrollTop += 0;
      if (menuResponsivo.className === "navegacao__itens") {
        menuResponsivo.className += " ativo";
      } else {
        menuResponsivo.className = "navegacao__itens";
      }
    });
  })();
  
  window.addEventListener("scroll", function() {
    if (pageYOffset >= 250) {
      document.querySelector(".navegacao").style.backgroundColor = "#e24e42";
      document.querySelector(".navegacao__itens").style.backgroundColor =
        "##e24e42";
    } else {
      document.querySelector(".navegacao").style.backgroundColor = "transparent";
      document.querySelector(".navegacao__itens").style.backgroundColor =
        "transparent";
    }
  });
  
  // altera a cor do texto quando passa o mouse
  $(function(){
  $('h1').hover(function(){
    $(this).css('color', '#eb6e80')
  
  });
  })
  
  // altera a cor do texto quando passa o mouse
  $(function(){
    $('.vida').hover(function(){
      $(this).css('background', '#ff8080')
  
    });
    })
  
  // caixa da autora
  try {
    const buttonAutora = document.querySelector('.arrow')
    buttonAutora.addEventListener('click', function(e){
        e.preventDefault()
        buttonAutora.classList.toggle('arrow-close')
        const autoraInfos = document.querySelector('.autora__about')
        autoraInfos.classList.toggle('opened');
    })
  } catch {
    if(!buttonAutora) {
        console.log('estamos na pagina home')
    }
  }
  
  // Efeito máquina de escrever
  function typeWriter(elemento){
    const textoArray = elemento.innerHTML.split('');
    elemento.innerHTML = '';
    textoArray.forEach((letra, i) => {
    setTimeout(function(){
      elemento.innerHTML += letra;
     }, 75 * i);
    });
  
  }
  
  const escrever = document.querySelector('h1');
  typeWriter(escrever);
  
  