const nome = prompt ('inserisci il tuo nome')
const cognome = prompt ('inserisci il tuo cognome')
const colore = prompt ('inserisci il tuo colore preferito')

const message = nome + cognome + colore + '23';

document.getElementById('output').innerHTML = message;