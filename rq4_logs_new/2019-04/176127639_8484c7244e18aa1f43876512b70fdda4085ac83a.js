export default validator = {};

validator.checkName = function(input){
  return /^[a-zA-Z][0-9a-zA-Z_]{4,16}[0-9a-zA-Z_]$/.test(input) || !input;
};
validator.checkPassword = function(input){
  return /^[a-zA-Z0-9_\-][0-9a-zA-Z_\-]{4,10}[0-9a-zA-Z_\-]$/.test(input) || !input;
};
validator.checkNumber = function(input){
  return /^[1-9][0-9]{6}[0-9]$/.test(input) || !input;
};
validator.checkPhone = function(input){
  return /^[1-9][0-9]{9}[0-9]$/.test(input) || !input;
};
validator.checkEmail = function(input){
  return /^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)*\.[a-zA-Z0-9]{2,6}$/.test(input) || !input;
};