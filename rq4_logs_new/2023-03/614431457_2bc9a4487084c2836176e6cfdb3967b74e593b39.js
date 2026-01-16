const Price = document.getElementById("price-amount");
const Tex = document.getElementById("tax-amount");
const TotalPrice = document.getElementById("total-amount");
const product = document.getElementById("product-amount");
// increase button event listener
const increaseBtn = document.getElementById("increase-btn");
increaseBtn.addEventListener("click", function(){
    const Increase = Number(product.innerText) + 1;
    product.innerText = Increase;
    const productPrice = Number(Price.innerText) + 1199;
    Price.innerText = productPrice;
    Number(Tex.innerText);
    const totalTax =  productPrice * 0.8/100;
    Tex.innerText = totalTax;
    const totalBill = productPrice + totalTax;
    Number(TotalPrice.innerText);
    TotalPrice.innerText = totalBill;
})
// decrease button event listener
const decreaseBtn = document.getElementById("decrease-btn");
decreaseBtn.addEventListener("click", function(){
    const Decrease = Number(product.innerText) - 1;
    if(Decrease < 0){
        productPrice = '0';
    }
    product.innerText = Decrease;
    const productPrice = Number(Price.innerText) - 1199;
    Price.innerText = productPrice;
    Number(Tex.innerText);
    const totalTex = Number(Price.innerText) * 0.8/100;
    Tex.innerText = totalTex;
    const totalBill = productPrice + totalTex;
    TotalPrice.innerText = totalBill; 
})