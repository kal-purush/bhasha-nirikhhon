var Complex = function(real, imaginary) {  
  this.real = real;
  this.imaginary = imaginary;
}

Complex.prototype = {  
  toString: function() {
  	return this.real + ' ' + this.imaginary + "i"; 
  },
  mult: function(c){
  	return new Complex(this.real * c.real - this.imaginary * c.imaginary,
  		this.real * c.imaginary + this.imaginary * c.real);
  },
  add: function(c){
  	return new Complex(this.real + c.real, this.imaginary + c.imaginary);
  },
  getAbsValueSquared: function (){
  	return this.real * this.real + this.imaginary * this.imaginary;
  },

  // Module of the trigonometric form of a complex number
  trigR: function(){
  	return Math.sqrt(Math.pow(this.real, 2) + Math.pow(this.imaginary, 2));
  },

  // Angle of the trigonometric form of a complex number
  trigAng: function(){
  	return Math.atan(this.imaginary / this.real); 
  },

// General formula for power a complex number to another complex number, but having c = 0 (first angular value).
// (a+bi)^(c+di)=r^c * e^(-dP) * [cos(d * ln r + c * P) + i sin(d * ln r + c * P)]
  power: function(c){
  	var r = this.trigR(),
  		p = this.trigAng();
  	var aux = Math.pow(r, c.real) * Math.pow(Math.E, -1 * c.imaginary * p);
  	return new Complex(aux * Math.cos(c.imaginary * Math.log(r) + c.real * p),
  		aux * Math.sin(c.imaginary * Math.log(r) + c.real * p));
  },

// Having the complex number expressed in trigonometric form:  r * e^(iP)
// ln{ r * e^(iP)} = ln(r) + i * P + 2kπi. with k = 0 --> ln{ r * e^(iP)} = ln(r) + i * P
  ln: function(){
  	var r = this.trigR(),
  		p = this.trigAng();
  	return new Complex(Math.log(r), p);
  }

}

var OperationalPoint = function(real, imaginary, frec, colourMethod){
	this.complex = new Complex(real, imaginary);
	this.frec = frec;
	this.colourMethod = colourMethod;
}

OperationalPoint.prototype = {
	getResult: function(){
		var result = this.frec.call(null, this.complex);
		return this.colourMethod.call(null, result);
	}
}