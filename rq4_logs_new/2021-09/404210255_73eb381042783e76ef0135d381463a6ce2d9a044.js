



function generar(){

    //obtenemos el valor de html, y le asignamos el valor de (y) y la variable columnas
    columnas = parseInt(document.getElementById("y").value);
    //obtenemos el valor de html, y le asignamos el valor de (x) a la variable filas
    filas = parseInt(document.getElementById("x").value);
    //creamos la matriz para las filas
    var array = [];
    //variable para obtener el numero mayor de la matriz
    var number_max = 0;
    //variable para obtener el numero menor de la matriz
    var number_min;
    //for para contar las filas que el usuario esta solicitando
    for(var i = 0; i < filas; i++){
        //creamos la matriz para las columnas
        var array2 = [];
        //creasmo un for dentro de otro for para contar las columnas que el usuario esta solicitando
        for(var j = 0; j < columnas; j++){
            //variable que se utilizara para generar el numero aleatorio con el metodo [math.random()*40]
            var random_number = Math.floor(Math.random() * 100);
            var number_min = random_number;
            //almacenamos los numeros aleatorio que se encuentran en la variable y los enviamos a la matriz2
            array2.push(random_number);

            if(number_max > random_number){
                number_max = random_number;
                
               
            }
            if (number_min < random_number){
                number_min = random_number;
                
            }
            
        }
       array.push("<tr><td>"+array2+"</td></tr><br>");

       
       
        
}

//var maximo = Math.max.apply(null,filas);
//var minimo = Math.max.apply(null,columnas);


console.log("Max: "+number_max+"Minimo: "+number_min);
var contenedor=document.getElementById("registro");
contenedor.innerHTML=document.write(array);
//contenedor.innerHTML+="numero Mayor"+maximo;

}