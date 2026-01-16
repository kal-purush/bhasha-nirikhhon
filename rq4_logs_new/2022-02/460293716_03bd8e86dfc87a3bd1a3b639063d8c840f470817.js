let $btngenerar = document.querySelector("#btngenerar");
$btngenerar.addEventListener('click',function(){
    getTable();
})

async function getTable(){
    var $arregloDeNumeros = document.getElementById("InputNumeros2").value;
    console.log($arregloDeNumeros);
    try{
        
        let answer = await fetch(`https://tdfrecuencia.herokuapp.com/decimales/${$arregloDeNumeros}`);
        let tablaRecibida = await answer.json();
        console.log("Objeto " + tablaRecibida)
        var arregloDeClases = new Array();
        let clave = 0;
        for(i=0;i<tablaRecibida.numeroDeClases;i++){
            clave++;
            let prueba = tablaRecibida.clasesAgrupadas[clave].desde;
            arregloDeClases.push(prueba);
        }

        var arregloDeClases2 = new Array();
        let clave2 = 0;
        for(i=0;i<tablaRecibida.numeroDeClases;i++){
            clave2++;
            let prueba = tablaRecibida.clasesAgrupadas[clave2].hasta;
            arregloDeClases2.push(prueba);
        }

        var limitesReales = new Array();
        let clave3 = 0;
        for(i=0;i<tablaRecibida.numeroDeClases;i++){
            clave3++;
            let prueba = tablaRecibida.limitesReales[clave3].desde;
            limitesReales.push(prueba);
        }

        var limitesReales2 = new Array();
        let clave4 = 0;
        for(i=0;i<tablaRecibida.numeroDeClases;i++){
            clave4++;
            let prueba = tablaRecibida.limitesReales[clave4].hasta;
            limitesReales2.push(prueba);
        }

        document.querySelector('#add').innerHTML += `
                
        <div class="col-1 titulo mb-5 mt-5">Clase</div>
        <div class="col-1 titulo mb-5 mt-5">Lim Inf</div>
        <div class="col-1 titulo mb-5 mt-5">Lim Sup</div>
        <div class="col-1 titulo mb-5 mt-5">LimiR Inf</div>
        <div class="col-1 titulo mb-5 mt-5">LimiR Sup</div>
        <div class="col-1 titulo mb-5 mt-5">Puntos Med</div>
        <div class="col-1 titulo mb-5 mt-5">Fre Abs</div>
        <div class="col-2 titulo mb-5 mt-5">Fre acum</div>
        <div class="col-3 titulo mb-5 mt-5">Fre relativa</div>
        `;

        for(i=0;i<tablaRecibida.numeroDeClases;i++){
            document.querySelector('#add').innerHTML+= `
            <div class="col-1">${(i+1)}</div>
            <div class="col-1">${arregloDeClases[i]}</div>
            <div class="col-1">${arregloDeClases2[i]}</div>
            <div class="col-1">${limitesReales[i]}</div>
            <div class="col-1">${limitesReales2[i]}</div>
            <div class="col-1">${tablaRecibida.puntosMedios[i]}</div>
            <div class="col-1">${tablaRecibida.frecuenciasAbsolutas[i]}</div>
            <div class="col-2">${tablaRecibida.frecuenciasAcumuladas[i]}</div>
            <div class="col-3">${tablaRecibida.frecuenciasRelativas[i]}</div>
            `;

        }
    }catch(e){

    }
}
