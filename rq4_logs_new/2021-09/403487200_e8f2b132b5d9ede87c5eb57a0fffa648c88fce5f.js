/* Crie uma função que receba um array de elementos e retorne um array somente com os números presentes no
array recebido como parâmetro. */

function filter(valores){
    numeros = []
    valores.forEach(element => {
        if(typeof element === "number")
            numeros.push(element)
    });
    console.log(numeros)
}
valores = [10, 20, "ola"]
filter(valores)