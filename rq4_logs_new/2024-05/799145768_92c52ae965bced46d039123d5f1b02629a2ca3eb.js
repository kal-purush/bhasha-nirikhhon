fetch('../db/produits.json')
    .then(response => response.json())
    .then(data => {
        const produitsDiv = document.querySelector('.produits');
        data.items.forEach(produit => {
            const produitElement = document.createElement('div');
            produitElement.classList.add('produit');

            const nomProduit = document.createElement('h3');
            nomProduit.textContent = produit.title;

            const prixProduit = document.createElement('p');
            prixProduit.textContent = `Prix : ${   Math.floor(produit.price.value*550)} fCFA`;

            const imgProduit = document.createElement('img');
            imgProduit.src = produit.imageLink;
            imgProduit.alt = produit.title;

            produitElement.appendChild(imgProduit);
            produitElement.appendChild(nomProduit);
            produitElement.appendChild(prixProduit);

            produitsDiv.appendChild(produitElement);
        });
    })
.catch(error => console.error('Une erreur s\'est produite : ', error));