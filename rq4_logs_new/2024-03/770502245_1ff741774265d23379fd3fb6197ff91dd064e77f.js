// Calcolo del prezzo finale con il Form

// Prendere i km dall'input nell'HTML
const kmInputElement = document.getElementById('km'); //Object | Null
console.log(kmInputElement)

// Prendere l'età dall'input nell'HTML
const etaInputElement = document.getElementById('eta');  // Object | Null
console.log(etaInputElement)

// €/km
const prezzoPerKm = 0.21;   // number

// Prendere il button per impostarlo come input button

const buttonElement = document.getElementById('submit')
console.log(buttonElement)

buttonElement.addEventListener('click', function() {
    // Raccolta dati
    const km = parseFloat(kmInputElement.value /* String */); // Number
    console.log('Km percorsi: ', km)

    const eta = parseInt(etaInputElement.value /* String */);  // Number
    console.log('Età: ')
    
    // Calcolare il prezzo al km
    const prezzoBase = km * prezzoPerKm; // number
    console.log(prezzoBase, typeof prezzoBase)

    if (eta < 18) {
        sconto = prezzoBase * 0.2
    } else if (eta > 65) {
        // -- ALTRIMENTI SE età passeggero >65 = sconto40%
        sconto = prezzoBase * 0.4
    } else {
        // -- ALTRIMENTI sconto = 0%
        sconto = 0
    }

    const prezzo = prezzoBase - sconto; // numebr 
    console.log(prezzo.toFixed(2), typeof prezzo, prezzo)

    // Prendere prezzo finale
    const prezzoElement = document.getElementById('prezzofinale')

    console.log('Invio dati')

    prezzoElement.innerHTML = 'Prezzo del biglietto: ' + prezzo.toFixed(2) + '&euro;'
})


