let allTotal = 0;

function addToCart(element){
	let mainEl = element.closest(".single-item");

	let price = mainEl.querySelector(".price").innerText;
	let price2 = parseInt(price.substring(1,price.lenght));
	let name = mainEl.querySelector("h3").innerText;
	let quantity = parseInt(mainEl.querySelector("input").value);

	let cartItems = document.querySelector(".cart-items");

	if(quantity>0)
	{
		cartItems.innerHTML += `<div class="cart-single-item">
									<h3>${name}</h3>
									<p>${price} x ${quantity} = $<span>${price2*quantity}</span></p>
									<button onClick ="removeFromCart(this)" class="remove-item">Ukloni</button>
								</div>
								`;

		allTotal+=price2*quantity;

		let total = document.querySelector(".total");
		total.innerText = `Total: $${allTotal} `;

		element.innerText = "Dodato";
		element.setAttribute('disabled','true');

	}
	
}

function removeFromCart(element) {
	let mainEl = element.closest(".cart-single-item");
	let price = parseInt(mainEl.querySelector("p span").innerText);
	let name = mainEl.querySelector("h3").innerText;
	let vegetables = document.querySelectorAll(".single-item");


	allTotal-=price;
	let total = document.querySelector(".total");
	total.innerText = `Total: $${allTotal} `;

	for (vegetable of vegetables){
		//console.log(vegetable);
		if (vegetable.querySelector(".si-content h3").innerText == name){
			vegetable.querySelector(".actions input").value = 0;
			//vegetable.querySelector(".actions button").disabled = "false";
			vegetable.querySelector(".actions button").removeAttribute('disabled');
			vegetable.querySelector(".actions button").innerText="Dodaj";
		}
	}

	mainEl.remove();
}