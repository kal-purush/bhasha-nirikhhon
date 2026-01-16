(function() {
  'use strict';

  // Do not write anything above this line and do not delete it


  // This is our note pad for testing JS
  // Write your code here

  var amount = prompt("Enter amount ", "");
  var rate=prompt("Enter rate","");
  var years=prompt("Enter years","");
  var si=(amount*rate*years)/100;
  alert("Simple interest is "+si);

}());