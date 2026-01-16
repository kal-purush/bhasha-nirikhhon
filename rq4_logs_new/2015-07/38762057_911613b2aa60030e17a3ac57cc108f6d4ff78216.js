define(['knockout', 'text!./color.html'], function(ko, template) {

  function ColorViewModel(params) {
    var qty = ko.observable(0);

    var color = params.color;
    
    var increment = function(){
      var tmp = qty();
      qty(++tmp);
    };

    var decrement = function(){
      var tmp = qty();
      qty(--tmp);
    };

    return {
      qty: qty,
      color: color,
      increment: increment,
      decrement: decrement
    };
  }
  return { viewModel: ColorViewModel, template: template };
});