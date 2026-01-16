
const produtos = [
    { nome: "Camiseta", valor: 29.99 },
    { nome: "Calça Jeans", valor: 49.99 },
    { nome: "Tênis", valor: 79.99 },
    { nome: "Boné", valor: 14.99 }
];

function renderProducts() {
    const productList = document.getElementById('product-list');
    productList.innerHTML = ''; 

    produtos.forEach(produto => {
        const productDiv = document.createElement('div');
        productDiv.className = 'product';

        const productName = document.createElement('h2');
        productName.textContent = produto.nome;

        const productPrice = document.createElement('p');
        productPrice.textContent = `R$ ${produto.valor.toFixed(2)}`;

        productDiv.appendChild(productName);
        productDiv.appendChild(productPrice);

        productList.appendChild(productDiv);
    });
}

renderProducts();