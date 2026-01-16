/* Escreva duas funções, uma para progressão aritmética e uma para progressão geométrica que recebam
como parâmetros um número n (número de termo), a1 (o primeiro termo) e r (a razão) e escreva os n termos ,
bem como a soma dos elementos.
 */
// duas funçoes 
//1 function progressãoAritmetica (n,a1,r)   
//  2 function progressãoGeométrica (n,a1,r)
// parametro    (n,a1,r)  primeiro termo ok ,numero de termos ok  ,   razao ok    ||
// falta an termo geral  que é o que vai escrever  e sn que é a soma dos termos.

//  escrever os n termos  an   | soma dos elementos. sn
// formula  an=a1+(n-1).r   primeiro tenho que fazer esta . 

function progressãoAritmetica (n,a1,r){
    var an = a1+(n-1)*r
    console.log (`Numero de termos ${an}`)
    var sn=(a1+an)*n/2
    console.log (`Soma dos elementos ${sn}`)
}

function progressãoGeometrica (n,a1,r){
    var an= a1*(r**n-1)
    console.log(`Numero de Termos ${an}`)
    var sn= an/(r-1)
    console.log(`Soma dos termos ${sn}`)
}


progressãoAritmetica(2,1,5)
progressãoGeometrica(2,1,5)