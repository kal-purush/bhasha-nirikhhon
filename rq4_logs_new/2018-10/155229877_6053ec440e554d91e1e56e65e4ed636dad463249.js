numero=Math.floor(Math.random()*100+1);
cadena="Elementos usados: ";
 max_tries=6;
console.log(max_tries);
tries=0;
band=false;
/* Esta funcion es la principal de nuestro JS y se encarga de analizar los datos introducidops por el usuario y meterlos en la variable "dato", compararlo con ell generado por la CPU, e ir dando pistan en base a ello para llevar al usuario a adivinar el numero*/
function compara(){ 
  if(band)
    return;
  var dato=document.getElementById('input1').value;//EXTRAEMOS EL DATO INSERTADO POR EL USUARIO
  if(dato!=='')
  {
      var numi=parseInt(dato);
      var box=document.getElementById('resultado');
      var texto1=document.getElementById('texto1');
      var num1=document.getElementById('num');
      if(numi===numero)
      {
        texto1.innerHTML="BIEN! has adivinado el numero";
        box.style.backgroundColor="blue";
        num1.style.color="black";
        num1.innerHTML=numero;
        document.getElementById('boton1').style.display="none";
        band=true;
      }
      else
      {
          /*Pista Si el Proporcionado por el usuario es mayor que el de la CPU*/
        if(numi>numero)
        {
          texto1.textContent="no no, el numero es menor de:";
          box.style.backgroundColor="red";
          num1.innerHTML=numi;
            tries++;
        }
        else /*Pista Si el Proporcionado por el usuario es menor que el de la CPU*/
        {
          texto1.innerHTML="MAL VAMOS, el numero es mayor de:";
          box.style.backgroundColor="red";
          num1.innerHTML=numi;
            tries++;
        }
      }
      box.style.display="block";
      document.getElementById('input1').value="";
      document.getElementById('input1').focus();
      cadena=cadena + " " + numi;
      console.log(cadena);
      var parrafo1=document.getElementById('usados');
      var parrafo2=document.getElementById('tries');
      parrafo1.innerHTML=cadena;
      if(tries>max_tries)
      {//si supera el numero de intentos permitidos
        texto1.innerHTML="Demasiados intentos realizados";
        box.style.backgroundColor="red";
        texto1.style.Color="yellow";
        num1.style.display="none";
        document.getElementById('boton1').style.display="none";
      }
      parrafo2.innerHTML="Intentos: " + tries;//indica el numero de intentos actual
  }
  else
    alert('Casilla vacia, Debe escribir un  numero entre 1 y 100');//aviso si la casilla esta vacia
}