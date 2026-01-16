class OrderStatusCtrl {
  svc: OrderStatusService;

  constructor(private OrderStatusService: OrderStatusService) {
    console.log("OrderStatusCtrl");
    this.svc = OrderStatusService;
  }

  ingredientsCsv(order: Order): string {
    var names: string[] = [];
    for (var i = 0; i < order.ingredients.length; i++) {
      names.push(order.ingredients[i].name);
    }
    return names.join(", ");
  }
}

class OrderStatusService {
  orders: Order[] = [
    {
      "drink_name": "Random Spirituous",
      "ingredients": [{
        "name": "peychauds bitters",
        "drops": 1
      }, {
        "name": "simple syrup",
        "parts": 1
      }, {
        "name": "gin",
        "parts": 4
      }],
      "total_oz": 2.5,
      "user_name": "argh"
    }
  ];

  constructor() {
    console.log("OrderStatusService");
  }
}

angular.module('nebree8.order-status', [])
  .service('OrderStatusService', OrderStatusService)
  .component('orderStatus', {
    templateUrl: 'components/order-status/order-status.html',
    controller: OrderStatusCtrl,
    controllerAs: 'ctrl',
  })