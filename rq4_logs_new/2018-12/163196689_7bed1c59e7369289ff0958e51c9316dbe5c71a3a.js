// Budget CONTROLLER
let budgetController = (function() {

    // Some code


})();




// UI CONTROLLER
let UIController = (function() {

   let DOMstrings = {
      inputType: '.add_type',
      inputDescription: '.add__description',
      inputValue: '.add__value',
      inputBtn: '.add__btn'
   }

    return {
      getInput: function() {
         return {
             type: document.querySelector(DOMstrings.inputType).value, // Will be either inc or exp
             description: document.querySelector(DOMstrings.inputDescription).value,
             value: document.querySelector(DOMstrings.inputValue).value
         };
      },

       getDOMstrings: function () {
          return DOMstrings;
       }
    };

})();



// GLOBAL APP CONTROLLER
let controller = (function(budgetCtrl, UICtrl) {

   let DOM = UICtrl.getDOMstrings();
   let ctrlAddItem = function() {

      // 1. Get the field input data
      let input = UICtrl.getInput();


      // 2. Add the item to the budget controller

      // 3. Add the item to the UI

      // 4. Calculate the budget

      // 5. Display the budget UI


   }

   document.querySelector(DOM.inputBtn).addEventListener('click', ctrlAddItem);

   document.addEventListener('keypress', function(event) {

      if(event.keyCode === 13 || event.which === 13) {
         ctrlAddItem();
      }

   });

})(budgetController, UIController);