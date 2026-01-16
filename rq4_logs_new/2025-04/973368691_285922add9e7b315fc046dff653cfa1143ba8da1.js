const pizzaContainer = document.querySelector(".pizzas-wrapper");
const basketaside = document.querySelector(".basket-aside");
const basketTitle = basketaside.querySelector("h2");
let totalArticles = 0;
let prixTotal = 0;

function addmoreless(count, cardpizzaElement, spanAdd, nom, prix, id) {
  const addcard = document.createElement("span");
  addcard.classList.add("add-to-cart-btn-after");
  addcard.style.display = "flex";

  const moins = document.createElement("img");
  moins.src = "../images/nono.svg";

  const counter = document.createElement("span");
  counter.textContent = count;
  counter.classList.add("add-to-cart-btn-after-more");

  const imgadd1 = document.createElement("img");
  imgadd1.src = "../images/sisi.svg";

  addcard.appendChild(moins);
  addcard.appendChild(counter);
  addcard.appendChild(imgadd1);

  addcard.dataset.id = id;
  addcard.dataset.nom = nom;
  addcard.dataset.prix = prix;
  addcard.dataset.count = count;

  imgadd1.addEventListener("click", () => {
    let newCount = parseInt(addcard.dataset.count) + 1;
    addcard.dataset.count = newCount;
    counter.textContent = newCount;
    totalArticles++;

    mettreAJourPanier();
  });

  moins.addEventListener("click", () => {
    let currentCount = parseInt(addcard.dataset.count);
    if (currentCount > 0) {
      let newCount = currentCount - 1;
      addcard.dataset.count = newCount;
      counter.textContent = newCount;
      totalArticles--;

      mettreAJourPanier();

      if (newCount === 0) {
        addcard.remove();
        spanAdd.style.display = "flex";

        if (totalArticles === 0) {
          resetBasketToEmpty();
        }
      }
    }
  });

  cardpizzaElement.appendChild(addcard);
}

function mettreAJourPanier() {
  const emptyBasket = basketaside.querySelector(".empty-basket");
  if (emptyBasket) {
    emptyBasket.style.display = "none";
  }

  basketaside.innerHTML = "";

  const basketContent = document.createElement("div");
  basketContent.classList.add("baskets-with-pizza");

  const title = document.createElement("h2");
  title.textContent = "Votre panier (" + totalArticles + ")";
  basketContent.appendChild(title);

  const ul = document.createElement("ul");
  ul.classList.add("basket-products");

  prixTotal = 0;

  const allAddedItems = document.querySelectorAll(".add-to-cart-btn-after");
  for (let i = 0; i < allAddedItems.length; i++) {
    const item = allAddedItems[i];
    const id = item.dataset.id;
    const nom = item.dataset.nom;
    const prixString = item.dataset.prix;
    const count = parseInt(item.dataset.count);

    if (count > 0) {
      const prixUnitaire = parseFloat(prixString.replace(" €", ""));
      const prixPourCettePizza = prixUnitaire * count;
      prixTotal += prixPourCettePizza;

      const li = document.createElement("li");
      li.classList.add("basket-product-item");
      li.dataset.id = id;

      const pizzaName = document.createElement("span");
      pizzaName.classList.add("basket-product-item-name");
      pizzaName.textContent = nom;

      const pizzaDetails = document.createElement("span");
      pizzaDetails.classList.add("basket-product-details");

      const quantity = document.createElement("span");
      quantity.classList.add("basket-product-details-quantity");
      quantity.textContent = count + "x";

      const unitPrice = document.createElement("span");
      unitPrice.classList.add("basket-product-details-unit-price");
      unitPrice.textContent = "@ " + prixUnitaire.toFixed(2) + " €";

      const totalPrice = document.createElement("span");
      totalPrice.classList.add("basket-product-details-total-price");
      totalPrice.textContent = prixPourCettePizza.toFixed(2) + " €";

      const removeIcon = document.createElement("img");
      removeIcon.classList.add("basket-product-remove-icon");
      removeIcon.src = "../images/remove-icon.svg";
      removeIcon.alt = "";

      removeIcon.addEventListener("click", () => {
        const pizzaItem = document.querySelector(
          `.pizza-item[data-id="${id}"]`
        );
        if (pizzaItem) {
          const currentCount = parseInt(item.dataset.count);
          totalArticles -= currentCount;

          const btnAfter = pizzaItem.querySelector(".add-to-cart-btn-after");
          if (btnAfter) {
            btnAfter.remove();
          }

          const addBtn = pizzaItem.querySelector(".add-to-cart-btn");
          if (addBtn) {
            addBtn.style.display = "flex";
          }

          if (totalArticles === 0) {
            resetBasketToEmpty();
          } else {
            mettreAJourPanier();
          }
        }
      });

      pizzaDetails.appendChild(quantity);
      pizzaDetails.appendChild(unitPrice);
      pizzaDetails.appendChild(totalPrice);

      li.appendChild(pizzaName);
      li.appendChild(pizzaDetails);
      li.appendChild(removeIcon);

      ul.appendChild(li);
    }
  }

  basketContent.appendChild(ul);

  const totalOrder = document.createElement("p");
  totalOrder.classList.add("total-order");

  const totalTitle = document.createElement("span");
  totalTitle.classList.add("total-order-title");
  totalTitle.textContent = "Total commande";

  const totalPriceElem = document.createElement("span");
  totalPriceElem.classList.add("total-order-price");
  totalPriceElem.textContent = prixTotal.toFixed(2) + " €";

  totalOrder.appendChild(totalTitle);
  totalOrder.appendChild(totalPriceElem);

  const deliveryInfo = document.createElement("p");
  deliveryInfo.classList.add("delivery-info");
  deliveryInfo.innerHTML = "Cette livraison est <span>neutre en carbone</span>";

  const confirmBtn = document.createElement("a");
  confirmBtn.classList.add("confirm-order-btn");
  confirmBtn.href = "#";
  confirmBtn.textContent = "Confirmer la commande";

  basketContent.appendChild(totalOrder);
  basketContent.appendChild(deliveryInfo);
  basketContent.appendChild(confirmBtn);

  basketaside.appendChild(basketContent);
}

function resetBasketToEmpty() {
  basketaside.innerHTML = `
    <h2>Votre panier</h2>
    <div class="empty-basket">
      <img src="../images/pizza.png" alt="panier vide" class="empty-basket-img" />
      <p>Votre panier est vide</p>
    </div>
  `;
  prixTotal = 0;
  totalArticles = 0;
}

function cardpizza(img, id, price, nom) {
  const cardpizza = document.createElement("div");
  cardpizza.classList.add("pizza-item");
  cardpizza.dataset.id = id;

  const imgpizza = document.createElement("img");
  imgpizza.classList.add("pizza-picture");
  imgpizza.src = img;
  imgpizza.alt = id;
  cardpizza.appendChild(imgpizza);

  const spanAdd = document.createElement("span");
  spanAdd.classList.add("add-to-cart-btn");
  const imgadd = document.createElement("img");
  imgadd.src = "../images/carbon_shopping-cart-plus.svg";
  spanAdd.textContent = "Ajouter au panier";
  spanAdd.appendChild(imgadd);
  cardpizza.appendChild(spanAdd);

  spanAdd.addEventListener("click", () => {
    spanAdd.style.display = "none";
    totalArticles++;
    addmoreless(1, cardpizza, spanAdd, nom, price, id);
    mettreAJourPanier();
  });

  const ul = document.createElement("ul");
  const li1 = document.createElement("li");
  const li2 = document.createElement("li");
  li1.textContent = nom;
  li2.textContent = price;
  li1.classList.add("pizza-name");
  li2.classList.add("pizza-price");
  ul.appendChild(li1);
  ul.appendChild(li2);

  cardpizza.appendChild(ul);
  pizzaContainer.appendChild(cardpizza);
}

window.addEventListener("DOMContentLoaded", async () => {
  let url = "http://51.38.232.174:3001/products";
  let response = await fetch(url);
  let data = await response.json();
  console.log(data);

  for (let j = 0; j < data.length; j++) {
    const dataelement = data[j];
    console.log(dataelement.image);
    cardpizza(
      dataelement.image,
      dataelement.id,
      dataelement.price + " €",
      dataelement.name
    );
  }
});