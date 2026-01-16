const sortPrice = function(a, b) {
  return a.price - b.price;
};

function onResponse(response)
{
  if(response.ok) {return response.json();}
  else
  {

    return  error() ;
  }
}
function error()
{
  document.getElementById("loading").style.display = "none";
  const btn = document.querySelectorAll('button');
  btn[0].textContent = "Error";
}
function onJsonReady(data)
{
  this.carInfo = data;
  this.printCars();
}
function printCars()
{
  const container = document.querySelector('#cards');
  container.innerHTML = '';
  for(const info of this.carInfo)
  {
    const car = new Car(container, info.maker, info.model, info.price);
  }
  const btn = document.querySelector('button');
  btn.textContent = "Loaded";
}

function sortCars(sortFunc) {
  this.carInfo.sort(sortFunc);
  this.printCars();
}
function load()
{
  document.getElementById("loading").style.display = "flex";
  const btn = document.querySelectorAll('button');
  btn[0].textContent = "Loading";
  fetch("http://demo4296963.mockable.io/listCars").catch(error).
  then(onResponse).then(onJsonReady);
}
class Car
{
  constructor(container, maker, model, price)
  {
    const card = document.createElement('div');
    const maker1 = document.createElement('span');
    const model1 = document.createElement('span');
    const price1 = document.createElement('span');
    maker1.innerHTML = maker;
    maker1.className = "title";
    model1.innerHTML = model;
    model1.className = "title";
    price1.innerHTML = price;
    price1.className = "price";
    card.className = "card";
    card.appendChild(maker1);
    card.appendChild(model1);
    card.appendChild(price1);
    container.appendChild(card);
  }
}

const btn = document.querySelectorAll('button');
for (const i of btn)
{
  i.addEventListener('click', load);}

document.getElementById("sortByPrice").onclick = function() {
  sortCars(sortPrice);
};