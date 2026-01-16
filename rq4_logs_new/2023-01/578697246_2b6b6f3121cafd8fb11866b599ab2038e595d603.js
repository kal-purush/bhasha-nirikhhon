//Imprima o índice e a lista com os seguintes números: 100, 200, 300, 400, 500, 600
const lista = [100, 200, 300, 400, 500, 600]
for (let i=0; i<lista.length; i++){
    console.log(`O número ${lista[i]} está na posição ${lista.indexOf(lista[i])}`)
}