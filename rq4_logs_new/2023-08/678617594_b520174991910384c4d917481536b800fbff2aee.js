const urlBase = "https://ecommercebackend.fundamentos-29.repl.co/";
const productsCatalog = document.querySelector(".products_container");
const menuCartContainer = document.querySelector(".cart-container");
const modalWindowDetails = document.querySelector(".modal_window_general");
const stockInCart = [];
let products = [];
let productsCopy = [];
let subtotalHtml = document.getElementById("subtotal");
let totalHtml = document.getElementById("total");
let ivaHtml = document.getElementById("iva");

initializePage();

function initializePage() {
    getApi();
    printCart(stockInCart);
}

async function getApi() {
    try {
        const data = await fetch(urlBase);
        const res = await data.json();
        products = JSON.parse(JSON.stringify(res));
        productsCopy = JSON.parse(JSON.stringify(res));       
        printProducts(res);
    } catch (error) {
        console.log(error);
    }
}

function printProducts(products) {
    let html = "";
    for (let product of products) {
        if(product.quantity === 0) {
            continue;
        }
        html += `
        <div class="product_card">
            <img src="${product.image}" alt="product_img" class="product_img">
            <p class="product_card_name">${product.name}</p>
            <p class="product_card_price">$${product.price.toFixed(2)}</p>
            <p class="product_card_stock">Stock: ${product.quantity}</p>
        <div class="product_card_footer">
        <div  class="add_btn">
        <button data-id="${product.id}" class="add_btn_main">
        Agregar al carrito
        </button>
        </div>
        <button data-id="${product.id}" class="add_btn_details">
        Detalles
        </button>
        </div>
        </div>
        `
    };
    productsCatalog.innerHTML = html;
    addEventsToProductButtons();
}

function printCart(shoppingCartProducts) {
    let subtotal = 0;
    let html = "";
    if (shoppingCartProducts.length > 0) {
      for (let product of shoppingCartProducts) {
        html += `
          <div class="menu_shopping_cart"> 
              <img src="${product.image}" alt="product_photo" class="img_in_cart">
              <div class="text_in_cart">
                  <p class="product_name">${product.name}</p>
                  <div class="manage_quantity">            
                    <a href="javascript:void(0)" onclick="updateCart(${product.id},'subtract')">-</a>
                    <div>
                      <p>${product.quantity}</p>
                    </div>
                    <a href="javascript:void(0)" onclick="updateCart(${product.id},'add')">+</a>
                  </div>
              </div>
              <div class="cart_product_right">
                <a href="javascript:void(0)" onclick="deleteProductInCart(${product.id})">x</a>
                <p>$${product.price*product.quantity}</p>            
              </div>
          </div>
          `;
        subtotal += Number(product.price) * Number(product.quantity);
      }
      menuCartContainer.innerHTML = html;
      subtotalHtml.innerHTML = "$" + subtotal;
      totalHtml.innerHTML = "$" + (subtotal * 1.16).toFixed(2);
      ivaHtml.innerHTML = "$" + (subtotal * 0.16).toFixed(2);
    } else {
      menuCartContainer.innerHTML = html;
      subtotalHtml.innerHTML = "$" + subtotal;
      totalHtml.innerHTML = "$0.00";
      ivaHtml.innerHTML = "$0.00";
    }
}

function addEventsToProductButtons() {
    const btnsAddToCart = document.querySelectorAll(".add_btn_main");
    for (i of btnsAddToCart) {
        i.addEventListener("click", addItemCart);
    }
    const btnsToggleDetails = document.querySelectorAll(".add_btn_details");
    for (i of btnsToggleDetails) {
        i.addEventListener("click", modalWindow);
    }
}

function modalWindow(event) {
    let html = "";
    if (event.target.classList.contains("add_btn_details")) {
        const productHtml = event.target.parentElement.parentElement;
        const productId = productHtml.querySelector("div").querySelector("button").getAttribute("data-id");
        const selectedProduct = products.find(product => product.id == productId);
        html += `        
        <div class="modal_window">
        <div class="modal_close_btn">
        <a onclick="close_modal()" class="close_modal">×</a>
        </div>
        <div>
        <img src="${selectedProduct.image}" alt="product_img" class="modal_img">
        <div class="modal_text">
        <div class="modal_name">${selectedProduct.name}</div>        
        <div class="modal_price">Precio: $${selectedProduct.price.toFixed(2)}</div>
        <div class="modal_stock">Stock: ${selectedProduct.quantity} pz</div> 
        <div class="modal_details">Descripción: ${selectedProduct.description}</div>      
        </div>   
        </div>    
        `
    }
    modalWindowDetails.style.display = "flex";
    modalWindowDetails.innerHTML = html;

}

function close_modal() {
    modalWindowDetails.style.display = "none";
}

function toggleShoppingCart() {
    const shoppingCartView = document.getElementById("cart-menu");
    if (shoppingCartView.style.display === "none") {
        shoppingCartView.style.display = "block"
    } else {
        shoppingCartView.style.display = "none";
    }
}

function addItemCart(event) {
    if (event.target.classList.contains("add_btn_main")) {
        const productHtml = event.target.parentElement.parentElement;
        const productId = productHtml.querySelector("div").querySelector("button").getAttribute("data-id");
        const selectedProduct = products.find(product => product.id == productId);
        if (selectedProduct.quantity > 0) {
            let productIndex = stockInCart.findIndex(productInCart => productInCart.id == selectedProduct.id);
            if (productIndex !== -1) {
                stockInCart[productIndex].quantity += 1
            } else {
                stockInCart.push({ ...selectedProduct, quantity: 1 });
            }
            selectedProduct.quantity--;
        } else {            
            alert("No hay unidades en existencia");
        }
    }
    printCart(stockInCart);
}

function openSidebar() {
    if (screen.width > 450) {
        document.getElementById("sidebar-id").style.width = "500px";
    } else {
        document.getElementById("sidebar-id").style.width = screen.width + "px";
    }
}

function closeSidebar() {
    document.getElementById("sidebar-id").style.width = "0";
}

function emptyCart() {
    while (stockInCart.length > 0) {
        stockInCart.pop()
    }
    menuCartContainer.innerHTML = ""
    products = JSON.parse(JSON.stringify(productsCopy));
    emptyReceipt();
}

function purchaseCart() {
    if (stockInCart.length === 0) {
        return;
    }
    while (stockInCart.length > 0) {
        stockInCart.pop()
    }
    menuCartContainer.innerHTML = ""
    productsCopy = JSON.parse(JSON.stringify(products));
    filterProducts();
    alert("¡Felicidades! ¡Tu compra ha sido realizada!");
    emptyReceipt();
}

function emptyReceipt() {
    subtotalHtml.innerHTML = "$0.00";
    totalHtml.innerHTML = "$0.00";
    ivaHtml.innerHTML = "$0.00";
}

function filterProducts() {
    const categorySelected = document.getElementById("products-filter").value;
    const productsFilter = products.filter(product => product.category === categorySelected);
    if (categorySelected !== "all") {
        printProducts(productsFilter)
    } else {
        printProducts(products);
    }
}

function updateCart(event, event2) {
    const selectedProduct = products.find((product) => product.id == event);
    const selectedProduct2 = stockInCart.find((product) => product.id == event);
    const productIndex = stockInCart.findIndex(
      (productInCart) => productInCart.id == selectedProduct.id
    );
    if (selectedProduct.quantity === 0 && event2 === "add") {
      alert("Ya no hay más stock disponible");
      return;
    }
    if (selectedProduct.quantity > 0 && event2 === "add") {
      stockInCart[productIndex].quantity += 1;
      selectedProduct.quantity--;
    } else if (selectedProduct2.quantity > 0 && event2 === "subtract") {
      if (selectedProduct2.quantity === 1) {
        stockInCart.splice(productIndex, 1);
      } else {
        stockInCart[productIndex].quantity--;
      }
      selectedProduct.quantity++;
    }
    printCart(stockInCart);
  }
  
  function deleteProductInCart(productId) {
    const selectedProduct = stockInCart.find(
      (product) => product.id == productId
    );
    const productIndex = stockInCart.findIndex(
      (productInCart) => productInCart.id == selectedProduct.id
    );
    stockInCart.splice(productIndex, 1);
  
    products.find((product) => product.id === productId).quantity +=
      selectedProduct.quantity;
    printCart(stockInCart);
  }