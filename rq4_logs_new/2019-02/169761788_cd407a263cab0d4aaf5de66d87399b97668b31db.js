
var next=document.getElementById('next');
var back=document.getElementById('back');
var menu=document.getElementById('menu');
//array contentos os texto dos  menus link
var links=['Quem Somos','O que Fazemos','Nosso time','projetos'];
// variavel para incrementa os id unicos para cada menu link
var id=0;
//for extraindo os indices do array link
for (link of links){
    id++;
//criando as li com o texto    
    var menuLi=document.createElement('li');
    var menuLinks=document.createTextNode(link);
    menuLi.setAttribute('class','links');  
    menuLi.appendChild(menuLinks);
    menu.appendChild(menuLi);
    menuLi.setAttribute('id',id);
}

/* variaves de controles */
    var menuLink;  
    var pages;
    var pegaPage;
    var backPage
    var atualPage;
    var inicio
    var Selecionado;
    var slide;

    slide=document.querySelectorAll('.home__tela div');
    /* obtendo ids dos menus */
    var idMenu1;
    idMenu1=document.getElementById('menu1');
    var idMenu2;
    idMenu2=document.getElementById('menu2');
    var idMenu3;
    idMenu3=document.getElementById('menu3');
    var idMenu4;
    idMenu4=document.getElementById('menu4');

/* variaveis de controles ativadas  */
    inicio=document.getElementById('1');
    inicio.setAttribute("class",'selected');
    menuLink=document.querySelectorAll('ul li.links');
    pegaPage=document.querySelector('li.selected');
    atualPage=pegaPage.id;
    if(atualPage==1){ 
        idMenu1.style.display="flex";
     }

   

 // SCRIPT PARA AVANÇAR O MENU  
    function nextMenu(){
// função que pega o id do menu atual pelo id da variavel pegaPage   
    pegouPage();
    atualPage++; 
//condição que controlar o incremento da variavel
//atualPage 
    if(atualPage==5){
        atualPage=4;
        ControlSlideMenu();
    }else{
        ControlSlideMenu();
        controlMenu();             
    }    
}


// SCRIPT PARA VOLTA O MENU 
    function backMenu(){
 // função que pega o id do menu atual pelo id da variavel pegaPage
        pegouPage();
        atualPage--; 
      //condição que controlar o decremento da variavel
      //atualPage   
        if(atualPage==0){
            atualPage=1;
            ControlSlideMenu(); 
        }else{
            ControlSlideMenu();
            controlMenu();        
        }       
}
    
 
// codígo teste beta para menu mais avançado

/* 
menuLink=document.querySelectorAll('.links');
function pegaMenu(){
    for(linksParaSerClicado of menuLink){
        var clicks= linksParaSerClicado.setAttribute('onclick','click();');  
        console.log('linksParaSerClicado ',linksParaSerClicado);} 
}*/

/* função que verificar qual a page atual para mostra na pagina as div relacionadas ao menus */
function controlMenu(){
    if(atualPage==1){
        idMenu1.style.display="flex";
        idMenu2.style.display="none";
        idMenu3.style.display="none";
        idMenu4.style.display="none";
    }else if(atualPage==2){
        idMenu1.style.display="none";
        idMenu2.style.display="flex";
        idMenu4.style.display="none";
        idMenu3.style.display="none";
    }else if(atualPage==3){
        idMenu1.style.display="none";
        idMenu2.style.display="none";
        idMenu3.style.display="flex";
        idMenu4.style.display="none";
    }else if(atualPage==4){
        idMenu1.style.display="none";
        idMenu2.style.display="none";
        idMenu3.style.display="none";
        idMenu4.style.display="flex";
    }
}

/* cotrole de next e back menu  */
function ControlSlideMenu(){
    backPage=document.querySelector("ul li.selected");   backPage.setAttribute('class','');
    backPage.setAttribute('class','links');
    backPage.setAttribute('onclick','');
    nextPage=document.getElementById(atualPage);
    nextPage.setAttribute("class",'selected');
    menuLink=document.querySelectorAll('ul li.links');
}
  
/* acionando controle do botoes de next e back */
    next.onclick=nextMenu;
    back.onclick=backMenu;

/* pegando a pageatual para saber qual o menu estamos */
function pegouPage(){
    pegaPage=document.querySelector('li.selected');
    atualPage=pegaPage.id;
}