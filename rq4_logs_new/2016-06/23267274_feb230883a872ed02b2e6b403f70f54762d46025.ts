class OrderStatusSheetCtrl {
  svc: OrderStatusService;

  constructor(private OrderStatusService: OrderStatusService) {
    console.log("OrderStatusSheetCtrl");
    this.svc = OrderStatusService;
  }
}

angular.module('nebree8.order-status-sheet', [])
  .controller('OrderStatusSheetCtrl', OrderStatusSheetCtrl);