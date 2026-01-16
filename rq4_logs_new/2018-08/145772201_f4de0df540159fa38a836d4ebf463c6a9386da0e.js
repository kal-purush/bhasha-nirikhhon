// Execute event on keyup in input boxes
var keyUpEvent = function () {
  updateDeposits(this);
  updateSubTotal(this);
  updateTotal(calcTotal());
};

// Update deposit array
var updateDeposits = function (curDOM) {
  // Find the index of the selected row's sub-title and update the count value
  var index = findArrayItem(curDOM, "name");
  // Get current input value
  var count = getInputNumVal(curDOM);
  window.deposits[index].count = count;
}

var findArrayItem = function (currentDOM, tdClassName) {
  // Get sub-title of the selected input row (e.g.: $5);
  var siblingText = $(currentDOM).parent().siblings().closest('td[class="' + tdClassName + '"]').text();
  // Find the index of the selected row's sub-title and update the count value
  return window.deposits.findIndex(function (deposit) {
    return deposit.name === siblingText;
  });
};

var getInputNumVal = function (currentDOM) {
  // Get current input value
  var textInputVal = $(currentDOM).val();
  // If current input value is an empty string, set it to 0
  return textInputVal === '' ? 0 : parseInt(textInputVal);
}

var updateSubTotal = function (curDOM) {
  // Find the index of the selected row's sub-title and update the count value
  var index = findArrayItem(curDOM, "name");
  // Get current input value (count)
  var count = getInputNumVal(curDOM);
  var subTotal = window.deposits[index].value * count;
  $(curDOM).parent().siblings().closest('td[class="subtotal"]').text('$' + subTotal);
};

// Calculate the total of the deposit array
var calcTotal = function () {
  return window.deposits.reduce(function (accum, obj) {
    return accum + (obj.value * obj.count);
  }, 0);
}

// Update the total on the table
var updateTotal = function (total) {
  $("td:contains('Total')").siblings().text('$' + total);
}



var makeSubTotTextNode = function (deposit) {
  var subTotal = deposit.value * deposit.count;
  return document.createTextNode('$' + subTotal);
};

// Create input for integers >= 0
var makeInputBox = function () {
  var input = document.createElement('input');
  input.setAttribute('type', 'number');
  input.setAttribute('min', 0);
  return input;
};

// Create each input row
var makeEachRow = function (trDOM, deposit, element) {
  var td = document.createElement('td');
  td.setAttribute('class', element);
  var child;

  switch (element) {
    case 'name':
      // Add each deposit array's name
      child = document.createTextNode(deposit[element]);
      break;
    case 'input':
      // Add input field
      child = makeInputBox();
      // Add keyup event
      child.addEventListener('keyup', keyUpEvent);
      break;
    case 'subtotal':
      child = makeSubTotTextNode(deposit);
      break;
  }

  td.appendChild(child);
  trDOM.appendChild(td);
  return trDOM;
};

// Create input rows
var makeTableRow = function (trDOM, deposit) {
  var trDOM;
  ['name', 'input', 'subtotal'].forEach(function (el) {
    trDOM = makeEachRow(trDOM, deposit, el);
  });
  return trDOM;
};



if (!window.createTBody) {
  window.createTBody = function (deposits) {
    // Create tbody element
    var tbody = document.createElement('tbody');
    deposits.forEach(function (item) {
      var tr = makeTableRow(document.createElement('tr'), item);
      tbody.appendChild(tr);
    });
    return tbody;
  };
}