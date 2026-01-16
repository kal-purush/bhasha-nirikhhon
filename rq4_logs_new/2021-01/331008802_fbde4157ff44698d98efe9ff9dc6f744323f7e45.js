var orderForm = document.getElementById('orderForm');
var order = document.getElementById('orders')
var arrayOfFoods = []
var total = 0;
var sumNumOforders = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];


function Order(name, number) {
    this.name = name;
    this.number = number;
    this.total = 0;
    arrayOfFoods.push(this);

}

orderForm.addEventListener('submit', submitOrder)
var row = document.createElement('tr')


function submitOrder(event) {

    event.preventDefault();


    var option = event.target.itemName.value;
    var quantity = event.target.quantity.value;

    var quantity1 = parseInt(quantity)
    total = total + quantity1;


    var newData= new Order(option, quantity)
    
    newData.render();
    makefooter();
    storeData();


}

Order.prototype.render = function (){

    var row = document.createElement('tr')
    tableEl.appendChild(row)
    var td1 = document.createElement('td')
    row.appendChild(td1)
    td1.textContent = this.name;
    var td1 = document.createElement('td')
    row.appendChild(td1)
    td1.textContent = this.number

}


function makefooter(){

    var table= document.createElement('table')
    var tableFinalRow = document.createElement('td');
    tableFinalRow.textContent = 'Totals';


    tableEl.appendChild(tableFinalRow);

    var totalDataCell = document.createElement('td');
    totalDataCell.textContent = total;
    tableEl.appendChild(totalDataCell)

    console.log(total)


}



var containar = document.getElementById('orders')
var tableEl = document.createElement('table')
containar.appendChild(tableEl)
function tableHeader() {
    var headerTable = document.createElement('thead')
    tableEl.appendChild(headerTable)
    var th1 = document.createElement('th')
    headerTable.appendChild(th1)
    th1.textContent = 'item'
    var th1 = document.createElement('th')
    headerTable.appendChild(th1)
    th1.textContent = 'Quantity'


}
function storeData() {
    localStorage.setItem('foodsorder', JSON.stringify(arrayOfFoods))
  }
  
  function checkAndRestore() {
    if (localStorage.length > 0) {
        arrayOfFoods = JSON.parse(localStorage.getItem('foodsorder'));
        storeData();



    }
  }




tableHeader();
checkAndRestore();