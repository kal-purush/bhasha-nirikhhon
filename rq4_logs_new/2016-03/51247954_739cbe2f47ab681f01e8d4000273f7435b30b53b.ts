/**
 * ShopOrderedController.ts
 *
 */

/// <reference path="../_references.ts" />

module IMCV.Koluki {
    "use strict";

    import ILocationService = angular.ILocationService;

    /**
     * Shop/Ordered controller.
     *
     */
    export class ShopOrderedController {

        private _orderRepository: IOrderRepository;
        private _$location: ILocationService;

        public static $inject = [
            "OrderRepository",
            "$location"
        ];

        /**
         * Creates a Shop/Ordered controller.
         *
         * @constructs
         * @param {IOrderRepository} orderRepository Order repository.
         */
        constructor(orderRepository: IOrderRepository, $location: ILocationService) {
            this._orderRepository = orderRepository;
            this._$location = $location;

            if (!orderRepository.reference) {
                $location.path("/");
            }
        }

        public get reference() {
            return this._orderRepository.reference;
        }

        public get fullName() {
            return this._orderRepository.fullName;
        }

        public get totalPayable() {
            return this._orderRepository.totalPayable;
        }

    }

}