class OrderRandomDrinkDialogCtrl {
  orderForm: ng.IFormController;  // Set from partial.
  userName: string;
  recipe: any;
  
  constructor(private $mdDialog: ng.material.IDialogService,
              private DrinksService: DrinksService,
              private RandomDrinkService: RandomDrinkService) {}

  submitDialog() {
    if (this.orderForm.$valid) {
      this.$mdDialog.hide(this.userName);
    }
  }
  
  orderSurprise() {
    if (this.orderForm.$valid) {
      this.RandomDrinkService.createDrink()
        .then((recipe: any) => {
          this.$mdDialog.hide(<any>{recipe: recipe, userName: this.userName});
        })
    }
  }

  preview() {
    window.console.log('Preview');
    this.RandomDrinkService.createDrink().then((recipe: any) => {
      window.console.log(recipe);
      this.recipe = recipe;
    });
  }

  ingredientsCsv(): string {
    return this.DrinksService.ingredientsCsv(this.recipe);
  }

  cancelDialog() {
    this.$mdDialog.cancel();
  }
}

angular.module('nebree8.order-drink')
  .controller('OrderRandomDrinkDialogCtrl', OrderRandomDrinkDialogCtrl)