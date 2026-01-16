// recupérer les produits dans le localstorage
let selectedProducts = JSON.parse(localStorage.getItem("product"));

let productsLocalStorage = [];
let cartItems = document.getElementById("cart__items");
console.log(selectedProducts);

//recupération de l'article avec l'api
async function displayProductInfo(id) {
  const result = await fetch(`http://localhost:3000/api/products/${id}`).then(
    (res) => res.json()
  );

  return result;
}

async function start() {
  // afficher un panier vide
  if (selectedProducts.length == 0) {
    let title = (document.querySelector("h1").textContent = "Panier Vide");

    // afficher les produits selectionnés
  } else {
    let title = (document.querySelector("h1").textContent = "Votre panier");
    let showItemsAtLocalStorage;
    for (const selectedProduct of selectedProducts) {
      const myData = await displayProductInfo(selectedProduct.productId);
      showItemsAtLocalStorage =
        (showItemsAtLocalStorage ? showItemsAtLocalStorage : "") +
        `
    <article class="cart__item" data-id="${selectedProduct.id}">
      <div class="cart__item__img">
        <img src="${myData.imageUrl}" alt="Photographie d'un canapé">
      </div>
      <div class="cart__item__content">
        <div class="cart__item__content__titlePrice">
          <h2>${myData.name}</h2>
          <p class="price">${myData.price}€</p>
          <p>${selectedProduct.productColor}</p>

        </div>
        <div class="cart__item__content__settings">
          <div class="cart__item__content__settings__quantity">
            <p>Qté : </p>
            <input type="number" id="${selectedProduct.id}"
             class="itemQuantity" name="itemQuantity" min="1"
             max="100" value="${selectedProduct.productQuantity}">
          </div>
          <div class="cart__item__content__settings__delete">
            <button id="${selectedProduct.id}" class="deleteItem">Supprimer</button>
          </div>
        </div>
      </div>
    </article>
    `;
    }
    cartItems.innerHTML = showItemsAtLocalStorage;
  }
}

function generateEventListener() {
  let inputNumber = document.querySelectorAll(".itemQuantity");
  inputNumber.forEach((input) => {
    input.addEventListener("click", (e) => {
      console.log(e);
      changeQuantity();
    });
  });
  // generer l'ensemble des eventlistener sur les boutons delete et sur les inputs
  // la fonction executé quand il y a un changement cest un callback
  // 1° l id 2° la cible

  // ecouter le bouton supprimer
  let removeBtn = document.querySelectorAll(".deleteItem");
  console.log(removeBtn);
  for (const btn of removeBtn) {
    btn.addEventListener("click", (e) => clickToDelete(e.target.id));
  }
}
// modifier la quantité du produit
let totalSelectedProducts = selectedProducts.length;

function changeQuantity() {
  for (let i = 0; i < totalSelectedProducts.length; i++) {}
  console.log("toto");
}

// Supprimer un article
function clickToDelete(id) {
  selectedProducts.splice(id, 1);
  localStorage.setItem("product", JSON.stringify(selectedProducts));
  let removeCard = document.querySelectorAll(".cart__item");
  for (const displayOff of removeCard) {
    displayOff.addEventListener("click", (e) => {
      displayOff.style.display = "none";
    });
  }
}

// récupérer les prix des produits et les additionner
async function sumProducts() {
  let totalPrice = document.getElementById("totalPrice");
  let totalCart = [];
  for (const selectedProduct of selectedProducts) {
    const myData = await displayProductInfo(selectedProduct.productId);
    totalCart.push(myData.price * selectedProduct.productQuantity);
  }

  const totalSum = totalCart.reduce((accumulator, currentValue) => {
    return accumulator + currentValue;
  }, 0);
  totalPrice.innerHTML = totalSum;
}

function sumArticles() {
  let totalQuantity = document.getElementById("totalQuantity");
  let totalArticle = [];
  let inputNumber = document.querySelectorAll(".itemQuantity");
  for (const number of inputNumber) {
    totalArticle.push(parseInt(number.value));
  }
  console.log(totalArticle);
  const totalSumQuantity = totalArticle.reduce((accumulator, currentValue) => {
    return accumulator + currentValue;
  }, 0);
  totalQuantity.innerHTML = totalSumQuantity;
}

await start();
generateEventListener();
await sumProducts();
sumArticles();