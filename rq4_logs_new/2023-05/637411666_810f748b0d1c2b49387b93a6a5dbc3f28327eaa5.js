let count = $("#count");
let mainSection = document.getElementById("checkout-section");//getting main element to add Dom tree

//getting values from local storage using json.parse
let value = JSON.parse(localStorage.getItem("Products"));

let totalPrice=0; 
//to get total price for all the items added in cart
for(let i=0; i<value.length;i++){
    totalPrice += (value[i].price)*(value[i].quantity);
}

//displaying number count for items added in cart
if(value==""||value==null){
    count.text("0");
    mainSection.innerHTML = `
    <h1 style="text-align: center">Cart is Empty</h1>
    <p style="text-align: center">Wanna place orders! <button id="add2cart">Click here</button></p>`
}
else{
    let index = (value.length)-1;
    count.text(value[index].allProductsCount);

    mainSection.innerHTML = `
    <h1>CheckOut</h1>
    <div id="checkout-wrapper">
        <div id="total-items">
            <h2>Total Items: ${value[index].allProductsCount}</h2>   
        </div>
        <div id="total-amount" class="card">
            <h2>Total Amount</h2>
            <p>Amount: Rs ${totalPrice}</p>
            <a href="./order.html"><button id="checkout">Place Order</button></a>
        </div>
    </div>`
}

$("#add2cart").click(function(){
    location.assign("./index.html");
})

//displaying cards for each item added in cart.
let totalItems= document.getElementById("total-items");
for(let j=0; j<value.length; j++){
    totalItems.innerHTML+=`
    <div class="card checkout-card">
        <div class="checkout-img-div">
            <img src="${value[j].preview}" alt="">
        </div>
        <div class="checkout-text-div">
            <h3>${value[j].name}</h3>
            <p>Quantity x ${value[j].quantity}</p>
            <p>Amount: Rs ${(value[j].price)*(value[j].quantity)}</p>
        </div>
    </div>`
}
//creating Object to send for an order creation.
function selectedProducts(){
    let objArray =[];
    for(let i=0; i<value.length; i++){
        let orderProduct ={
            productName: value[i].name,
            quantity: value[i].quantity,
            totalPrice: (value[i].quantity) * (value[i].price)
        }
        objArray.push(orderProduct);
    }
    return objArray;
}
//POST Request function
function postCall(data){
    $.ajax({
        type:"POST",
        // url: "https://6139330a1fcce10017e78a63.mockapi.io/shopLaneOrder",
        url: "https://5d76bf96515d1a0014085cf9.mockapi.io/order",
        data: JSON.stringify(data),
        contentType: "application/JSON",
        success: function(response){
            console.log(response);
            location.assign("./order.html")
        },
        error: function(error){
            console.log(error)
        }
    })
}
//adding onclick event to placeOrder button
$("#checkout").click(function(){
    let orderProduct = {
        sanky2020ProductOrder: selectedProducts()
    }
    postCall(orderProduct)
})