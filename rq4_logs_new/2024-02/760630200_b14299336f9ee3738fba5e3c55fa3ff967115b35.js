/*Flechas de carrusel */

const rightBtn = document.querySelector("#boton-izquierda");
const leftBtn = document.querySelector("#boton-derecha");
const content = document.querySelector(".productos-desli");

rightBtn.addEventListener("click", () => {
    content.scrollLeft += 200;
});

leftBtn.addEventListener("click", () => {
    content.scrollLeft -= 200;
});


/*===============================*/

const btnCart = document.querySelector(".container-compras");
const containerCartProducts = document.querySelector(".container-cart-products");

btnCart.addEventListener("click", () => {
    containerCartProducts.classList.toggle
    ("hidden-cart")
});

/*===============================*/

const cartInfo = document.querySelector(".cart-product")
const rowProduct = document.querySelector(".row-product")

const productsList = document.querySelector(".productos-desli")

let allProducts = []


const valorToltal = document.querySelector(".total-pagar")

const countProducts = document.querySelector("#contador-productos")



productsList.addEventListener("click", e => {

    if(e.target.classList.contains("btn-primary")){
        const product = e.target.parentElement


        const infoProduct = {
            quantity: 1,
            title: product.querySelector("h5").textContent,
            price: product.querySelector("p").textContent,
        };

        const exits = allProducts.some(product => product.title === infoProduct.title)
        
        if (exits){
            const products = allProducts.map(product => {
                if(product.title === infoProduct.title){
                        product.quantity++;
                        return product
                } else{
                    return product
                }
            })

            allProducts = [...products]

        } else{

            allProducts = [...allProducts, infoProduct];

        }


        

        showHTML();

    }


});

rowProduct.addEventListener("click", e => {
    if(e.target.classList.contains("icon-close")){
        const product = e.target.parentElement;
        const title = product.querySelector("p").textContent;

        allProducts = allProducts.filter (product => product.title !== title)
    };

    console.log(allProducts)
    showHTML()
})


const showHTML = () => {


    if(!allProducts.length){
        containerCartProducts.innerHTML=`
             <p class="cart-empty">El carrito esta vacio</p>
        `
    }

    rowProduct.innerHTML = "";

    let total = 0;
    let totalOfproducts = 0;


     allProducts.forEach(product => {
        const containerProduct = document.createElement("div")
        containerProduct.classList.add("cart-product")

        containerProduct.innerHTML = `
        <div class="info-cart-product">
        <span class="cantidad-producto-carrito">
          ${product.quantity}
        </span>
        <p class="titulo-producto-carrito">
          ${product.title}
        </p>
        <span class="precio-producto-carrito">
          ${product.price}
        </span>

      </div>

      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="1.5"
        stroke="currentColor" class="icon-close">
        <path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12" />
      </svg>

    </div>
        `;

        rowProduct.append(containerProduct);

        total = total + parseInt(product.quantity * product.price.slice(1))
        totalOfproducts = totalOfproducts + product.quantity;
     });

      valorToltal.innerText = `$${total}`
      countProducts.innerText = totalOfproducts;

};