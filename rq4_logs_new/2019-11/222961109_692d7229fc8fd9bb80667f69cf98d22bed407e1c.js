var somma = 0;
// per fare la somma creo già la variabile fuori  perchè si aggiorni a ogni ciclo con la somma corrente, metto la richiesta prompt direttamente nel ciclo
for (var i = 0; i < 5; i++) {
    numeriUtente = parseInt(prompt("Giochiamo,inserisci un numero"));
    somma = somma + numeriUtente;
}
console.log(somma);