function somaarray(array) {
    let soma = 0;
    for (let i = 0; i < array.length; i++) {
        soma += array[i]; 
    }
    return soma;
}

const numeros = [1, 2, 3, 4, 5];
const resultado = somaarray(numeros);



function maiorelementodasilva(array) {
    if (array.length === 0) {
        return null; 
    }
    
    let maior = array[0]; 
    
    for (let i = 1; i < array.length; i++) {
        if (array[i] > maior) {
            maior = array[i]; 
        }
    }
    
    return maior; 
}



function Pares(array) {
    let contador = 0; 

    for (let i = 0; i < array.length; i++) {
        if (array[i] % 2 === 0) {
            contador++; 
        }
    }

    return contador; 
}



function Media(array) {
    if (array.length === 0) {
        return 0;
    }

    let soma = 0; 

    for (let i = 0; i < array.length; i++) {
        soma += array[i]; 
    }

    return soma / array.length; 
}





function contarpositivosnegativos(array) {
    let positivos = 0;
    let negativos = 0;

    for (let numero of array) {
        if (numero >= 0) {
            positivos++;
        } else {
            negativos++;
        }
    }

    console.log(`Números positivos: ${positivos}`);
    console.log(`Números negativos: ${negativos}`);
}

const novoArray = [3, -1, 0, 7, -5, 2, -3];



function multiplicar(array, numero) {
    return array.map(elemento => elemento * numero);
}

const meuArray = [1, 2, 3, 4];
const numero = 2;
const resultado1 = multiplicar(meuArray, numero);

