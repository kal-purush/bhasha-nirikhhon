'use strict';

// Array of stores
var allStoresArray = [];

// Array of hours the stores are open
var openHoursArray = ['6:00 - 7:00am', '7:00 - 8:00am', '8:00 - 9:00am', '9:00 - 10:00am', '10:00 - 11:00am', '11:00 - 12:00pm', '12:00 - 1:00pm', '1:00 - 2:00pm', '2:00 - 3:00pm', '3:00 - 4:00pm', '4:00 - 5:00pm', '5:00 - 6:00pm', '6:00 - 7:00pm', '7:00 - 8:00pm'];
 //Access the DOM at the table on the sales page
var salesTable = document.getElementById('sales');

//Access the form in the DOM
var salesForm = document.getElementById('new_store_form');

// CookieHut constructor function
function CookieHut(identifier, storeName, storeAddress, storePhone, minCustsPerHour, maxCustsPerHour, avgCookiesPerCust) {
  this.identifier = identifier; // identifier should be all lowercase
  this.storeName = storeName;
  this.storeAddress = storeAddress;
  this.storePhone = storePhone;
  this.minCustsPerHour = minCustsPerHour;
  this.maxCustsPerHour = maxCustsPerHour;
  this.avgCookiesPerCust = avgCookiesPerCust;
  this.custsPerHourArray = [];
  this.cookiesPerHourArray = [];
  this.dailySalesTotal = 0;

  allStoresArray.push(this);

  // Updates a store's array of custsPerHourArray with a random number of customers per hour for each hour that the store is open.
  this.generateHourlyTraffic = function() {
    for (var i = 0; i < openHoursArray.length; i++) {
      var customers = getRandomIntInclusive(this.minCustsPerHour, this.maxCustsPerHour);
      // console.log('Customers per hour:', customers);
      this.custsPerHourArray[i] = customers;
    }
  };

  // Updates a store's array of cookiesPerHourArray by multiplying each item in a store's custsPerHourArray array by the average number of cookies sold per customer for that store.
  this.projectedSales = function() {
    this.generateHourlyTraffic();
    var hourlyCookieAverage = this.avgCookiesPerCust;
    var totalCookies = 0;
    for (var i = 0; i < openHoursArray.length; i++) {
      var cookies = Math.floor(this.custsPerHourArray[i] * this.avgCookiesPerCust);
      // console.log('Cookies this hour:', cookies);
      totalCookies += cookies;
      this.cookiesPerHourArray[i] = cookies;
    }
    this.dailySalesTotal = totalCookies;
    return totalCookies;
  };

  // Display the sales numbers for a given store as a list.
  // ADD IDS TO THE ROWS AS THEY'RE CREATED
  this.render = function(rowColorCheck) {
    var salesTotal = this.projectedSales();
    var trEl = document.createElement('tr');
    trEl.id = this.identifier;
    if((rowColorCheck % 2) === 0) {
      trEl.className = 'grey';
    }
    var tdEl = document.createElement('td');
    tdEl.className = 'v_header';
    tdEl.textContent = storeName;
    trEl.appendChild(tdEl);
    buildElement('td', salesTotal, trEl);
    for(var i = 0; i < this.cookiesPerHourArray.length; i++) {
      buildElement('td', this.cookiesPerHourArray[i], trEl);
    }
    salesTable.appendChild(trEl);
  };
};

// Create instance of CookieHut for all existing stores.
var firstAndPike = new CookieHut('firstandpike', '1st and Pike', '102 Pike St, Seattle, WA 98101', '206-xxx-xxxx', 23, 65, 6.3);

var seatac = new CookieHut('seatac', 'Seatac Airport', 'Concourse D, 17801 International Blvd, Seattle, WA 98158', '425-xxx-xxxx', 3, 24, 1.2);

var seattleCenter = new CookieHut('seattlecenter', 'Seattle Center', '305 Harrison St, Seattle, WA 98109', '206-xxx-xxxx', 11, 38, 3.7);

var capitolHill = new CookieHut('capitolhill', 'Capitol Hill', '434 Broadway Avenue E, Seattle, WA 98102', '206-xxx-xxxx', 20, 38, 2.3);

var alki = new CookieHut('alki', 'Alki', '2742 Alki Ave SW; Seattle, WA 98116', '206-xxx-xxxx', 2, 16, 4.6);

// Returns a random integer between min (included) and max (included)
function getRandomIntInclusive(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}
// Functions to populate the sales table with projected sales information

// UPDATE EXISTING FUNCTIONS WITH THIS SO THEY'RE SHORTER!
// Builds an element and adds it to another element, attribute optional
function buildElement(kind, content, where, attName, attValue) {
  var x = document.createElement(kind);
  x.textContent = content;
  if(attName && attValue) {
    x.setAttribute(attName, attValue);
  }
  where.appendChild(x);
}

// Generate header row
function makeHeaderRow() {
  var trEl = document.createElement('tr');
  trEl.id = 'table_header';
  buildElement('th', '', trEl);
  buildElement('th', 'Daily Total', trEl);
  for(var i = 0; i < openHoursArray.length; i++) {
    buildElement('th', openHoursArray[i], trEl);
  }
  salesTable.appendChild(trEl);
}

// Generate footer row
// ADD CLASSES TO THE CELLS IN THE FOOTER ROW
function makeFooterRow() {
  var trEl = document.createElement('tr');
  trEl.className = 'table_footer';
  buildElement('td', '', trEl); // blank space in footer row
  var grandTotal = 0;
  for(var i = 0; i < allStoresArray.length; i++) {
    grandTotal += allStoresArray[i].dailySalesTotal;
  }
  buildElement('td', grandTotal, trEl);
  for(var i = 0; i < openHoursArray.length; i++) {
    var totalByHour = 0;
    for(var j = 0; j < allStoresArray.length; j++) {
      totalByHour += allStoresArray[j].cookiesPerHourArray[i];
    }
    buildElement('td', totalByHour, trEl);
  }
  salesTable.appendChild(trEl);
}

// Generate sales numbers for each store in an array of stores
function generateSalesNumbers() {
  for(var i = 0; i < allStoresArray.length; i++) {
    // console.dir(allStoresArray[i]);
    allStoresArray[i].render(i);
  }
}

// Adds a new store via the 'sales' form on sales.html
function handleNewStoreSubmit(event){
  event.preventDefault();

  var identifier = event.target.identifier.value;
  var storeName = event.target.storeName.value;
  var storeAddress = event.target.storeAddress.value;
  var storePhone = event.target.storePhone.value;
  var minCustsPerHour = event.target.minCustsPerHour.value;
  var maxCustsPerHour = event.target.maxCustsPerHour.value;
  var avgCookiesPerCust = event.target.avgCookiesPerCust.value;

  if (!identifier || !storeName || !storeAddress || !storePhone || !minCustsPerHour || !maxCustsPerHour || !avgCookiesPerCust) {
    return alert('Fields cannot be empty!');
  }

  var newStore = new CookieHut(identifier, storeName, storeAddress, storePhone, minCustsPerHour, maxCustsPerHour, avgCookiesPerCust);

  salesTable.innerHTML = '';

  makeHeaderRow();
  generateSalesNumbers();
  makeFooterRow();

  event.target.identifier.value = null;
  event.target.storeName.value = null;
  event.target.storeAddress.value = null;
  event.target.storePhone.value = null;
  event.target.minCustsPerHour.value = null;
  event.target.maxCustsPerHour.value = null;
  event.target.avgCookiesPerCust.value = null;
};

// Event listener to add a new store
new_store_form.addEventListener('submit', handleNewStoreSubmit);

makeHeaderRow();
generateSalesNumbers();
makeFooterRow();