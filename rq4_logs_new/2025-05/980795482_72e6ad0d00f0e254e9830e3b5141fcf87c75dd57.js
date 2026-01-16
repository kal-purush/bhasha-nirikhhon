window.onload = generarCarta;
function generarCarta() {
    let elementos = ['♦', '♥', '♠', '♣'];
    let valores = ['A', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K'];

    let elemento = elementos[randomNumberFromArray(elementos)];
    let valor = valores[randomNumberFromArray(valores)];

    let cartaHTML = `
        <div class="card ${getElementoClass(elemento)}">
            <span class="top">${elemento}</span>
            <span class="number">${valor}</span>
            <span class="bottom">${elemento}</span>
        </div>
    `;

    document.getElementById("card").innerHTML = cartaHTML;
}

function randomNumberFromArray(array) {
    return Math.floor(Math.random() * array.length);
}

function getElementoClass(elemento) {
    if (elemento === '♦') return 'fuego';
    if (elemento === '♥') return 'agua';
    if (elemento === '♠') return 'tierra';
    if (elemento === '♣') return 'aire';
    return '';
}