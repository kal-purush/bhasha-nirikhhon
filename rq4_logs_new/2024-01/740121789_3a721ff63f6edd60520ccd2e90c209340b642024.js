const $cartProductTemplate= document.querySelector("#cart-product-template");
const $cartContainer= document.querySelector(".renderProducts");
const $fragment= document.createDocumentFragment();

const cartList = [ ];
const cartPrice= 0;

const renderBuyList= ()=>{
    $cartContainer.innerHTML= ""
    cartList.forEach((element)=>{
        
        const $clone= document.importNode($cartProductTemplate.content, true);
        $clone.querySelector(".img").src= `${element.product.img}`;
        $clone.querySelector(".title").innerText= `${element.product.title}`;
        $clone.querySelector(".productPrice").innerText= `${element.product.price}`;
        $clone.querySelector(".productUnits").value= `${element.units}`
        $fragment.appendChild($clone)
    });
    $cartContainer.appendChild($fragment)
}



const renderFavList= ()=>{

}

export {renderBuyList, renderFavList, cartList, cartPrice}