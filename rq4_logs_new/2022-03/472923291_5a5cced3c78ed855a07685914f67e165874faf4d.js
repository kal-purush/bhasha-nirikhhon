const images = [

    'build/img/nic1.jpg',
    'build/img/Opnic2.jpg',
    'build/img/Opnic3.jpg',
    'build/img/Opnic4.jpg',
    'build/img/Opnic5.jpg',
    'build/img/Opnic6.jpg',
    'build/img/Opnic7.jpg',
    'build/img/Opnic8.jpg',
    'build/img/Opnic9.jpg',
    'build/img/Opnic10.jpg',
    'build/img/Opnic11.jpg'

];

const back = document.querySelector('.main-content');
let start = 0;

//Modo no correcto ya que no puede modificarse la transition
// function cambiarFondo(){

    
//     if( start == images.length ){
        
//         start = 0;

//     } 

//     //"url('build/img/nicolaitaFAVICON.png') no-repeat " Entrada Original
//     back.style.background = `linear-gradient( rgba( 0,0,0, 0.7 ), rgba( 0,0,0, 0.7 ) ), url( ${images[start]} )`;
//     back.style.backgroundSize = 'cover';

//     setTimeout( () => {
//         back.style.transition = 'all 1s';
//         back.style.opacity = '1';
//     } ,1000)

//     start++;

// }

// let changingImage = setInterval( cambiarFondo, 3000 );

//Agregando las imágenes de fondo como nuevos elementos dinámicos
images.forEach( (element, index) => {

    const contenido = document.createElement( 'DIV' );
    contenido.classList.add( 'imagenContenido' );
    contenido.style.position = 'absolute';
    contenido.style.width = '100%';
    contenido.style.height = '100%';
    contenido.style.top = '0';
    contenido.style.left = '0';
    contenido.style.opacity = '0';
    contenido.style.background = ` url( ${images[index]} )`;
    contenido.style.backgroundSize = 'cover';
    contenido.style.backgroundRepeat = 'no-repeat';
    contenido.style.backgroundPosition = 'center';
    contenido.style.zIndex = '-2';
    // contenido.style.animation = 'mostrar 2s ease'; //no necesario
    
    contenido.style.transition = 'all 3s ease';
    back.appendChild( contenido );

} );


//Mostrando las imágenes una a una y desapareciéndolas si ya no son la que está en la cima de la pila
function mostrarImagen(){

    const imgs = document.querySelectorAll('.imagenContenido');
    
    if( start == imgs.length ) {
        
        start = 0;

    }
    
    imgs[start].style.opacity = '1';
    
    imgs.forEach( (element, index) => {

        if( index != start ){

            element.style.opacity = '0';

        }

    } );

    start++;

    // console.log( start );

}


mostrarImagen();
setInterval( mostrarImagen, 5000 );


//necesario modificar opacidad únicamente, en vez de modificar el background o display, ya que con esas propiedades no se podría hacer uso de transition en los elementos dinamicos.
//el fondo con opacidad en negro se hace con el elemento before del contenedor principal de forma que siempre se mantenga con el mismo color, ya que si se agregara el background en el pseudo elemento usando un gradiente linear además del url, eso haría que en la transición del opacity se volviera blanco, generando un efecto raro.