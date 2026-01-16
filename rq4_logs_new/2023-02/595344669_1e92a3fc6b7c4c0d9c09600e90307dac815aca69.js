//https://kea-alt-del.dk/t7/api/products

//1 hente data
async function getData() {
  const response = await fetch("https://kea-alt-del.dk/t7/api/products?limit=25");
  const data = await response.json();
  console.log(data);
  //2 loope//3 for hver af dem
  data.forEach(showProduct);
}

getData();

function showProduct(product) {
  console.log(product);
  //4 fange vores template

  const template = document.querySelector("#smallProductTemplate");
  //5 clone
  const copy = template.cloneNode(true);

  //6 skifte data
  copy.querySelector("h3").textContent = product.productdisplayname;

  //7 appende
  document.querySelector("main").appendChild(copy);
}