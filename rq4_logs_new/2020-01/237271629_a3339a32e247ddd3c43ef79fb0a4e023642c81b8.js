//Cach root
const renderDiv = document.querySelector("#renderDiv")

//Skapar upp en produkt mall
const newProduct = document.createElement("div")
newProduct.className = "productDiv"

const newImg = document.createElement("img")
newImg.className = "productImg"
newProduct.appendChild(newImg)

const newName = document.createElement("h1")
newName.className = "productName"
newProduct.appendChild(newName)

const newPrice = document.createElement("h3")
newPrice.className = "productPrice"
newProduct.appendChild(newPrice)

for (let i = 0; i < 10; i++) {
    const product = newProduct.cloneNode(true)
    renderDiv.appendChild(product)
}