(function() {
    'use strict';
    angular.module('shoppingapp', [])
        .controller('buycontroller', buycontroller)
        .controller('boughtcont', boughtcont)
        .service('listservice', listservice);

 
    buycontroller.$inject = ['listservice'];

    function buycontroller(listservice) {
        var toBuyList = this;
        toBuyList.items = listservice.getitems();

        toBuyList.buyItem = function(itemIndex) {
            listservice.buyItem(itemIndex);
        };
    }

    boughtcont.$inject = ['listservice'];
    function boughtcont(listservice) {
        var alreadyBougthList = this;

        alreadyBougthList.items = listservice.getboughtitems();
    }
    function listservice() {
        var service = this;
        var toBuyItems = [
            { name: "cookies",  quantity: 5 },
            { name: "ice cream", quantity: 5 },
            { name: "fish", quantity: 1 },
            { name: "lassi", quantity: 1 },
            { name: "cheese block", quantity: 1 }
            
        
        ];
        var alreadyBoughtItems = [];
    service.buyItem = function(itemIndex) {
            var item = toBuyItems[itemIndex];

            alreadyBoughtItems.push(item);
            toBuyItems.splice(itemIndex, 1);
        };

        service.getitems = function() {
            return toBuyItems;
        };
        service.getboughtitems = function() {
            return alreadyBoughtItems;
        };
    }

  
})();