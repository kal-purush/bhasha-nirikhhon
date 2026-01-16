var myMod= require('../index.js');

function Foo(val,status, model,invalidAttributes) {
  this.value = val;
  this.status= status;
  this.model = model;
  this.invalidAttributes= invalidAttributes;
}
Foo.prototype.nifty = "I'm the nifty inherited property.";
Foo.prototype.toJSON = function() {
  var x = {
  	value:this.value,
  	status:this.status,
  	model:this.model,
  	invalidAttributes: this.invalidAttributes
  }
  return x;
};

var obj = new Foo(42, 400, 'stephany',{one:'culito',two:'pene'});


console.log('respuesta ' + JSON.stringify(myMod(obj)));