let $calculate = document.getElementById('calculate');
let methods = [],
    times = [],
    quantity = [];

$calculate.addEventListener('click', function(e) {
  e.preventDefault();
  
  /**
   * Retrieving methods selected
   */
  let $methods = document.querySelectorAll('input[name="method"]:checked');
  
  if ($methods.length <= 0) {
    alert('Selecione pelo menos 1 método de ordenação'); 
    return;
  }
  
  /**
   * Retrieving how many times it will be executed
   */
  let $times = document.querySelectorAll('input[name="times"]:checked');
  
  if ($times.length <= 0) {
    alert('Não foi escolhido nenhuma quantidade de repetições');
    return;
  }
  
  /**
   * Retrieving the quantity of elements 
   */  
  let $quantity = document.querySelectorAll('input[name="quantity"]:checked');
  
  if ($quantity.length <= 0) {
    alert('Não foi escolhido nenhuma quantidade de elementos');
    return;
  }
  
  /** If pass here, everything was choosen */
  
  for (var i = 0; i < $methods.length; i++) {
    methods.push($methods[i].value); //VSCode shows an error, but it doesn't exists    
  }
  for (var i = 0; i < $times.length; i++) {
    times.push($times[i].value); //VSCode shows an error, but it doesn't exists    
  }
  for (var i = 0; i < $quantity.length; i++) {
    quantity.push($quantity[i].value); //VSCode shows an error, but it doesn't exists    
  }
  
  console.log(methods);
  console.log(times);
  console.log(quantity);
  
});