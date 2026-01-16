
var imagen=document.getElementsByClassName('imagen');
var modal=document.getElementsByClassName('modal')[0];
var img=document.getElementById('image');
var cerrar=document.getElementsByClassName('cerrar');

for (var i=0;i<imagen.length;i++){
  imagen[i].addEventListener('click',disparaImagen);
}
for (var i=0;i<cerrar.length;i++){
  cerrar[i].addEventListener('click',nodisparaImagen);
}
// cerrar.addEventListener('click',nodisparaImagen);

function disparaImagen(){
   modal.style.display="block";
   img.src=this.src;
 }
 function nodisparaImagen() {
   this.style.display="none";
 }