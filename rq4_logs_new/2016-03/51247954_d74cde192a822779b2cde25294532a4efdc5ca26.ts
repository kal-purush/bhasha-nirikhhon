/**
 * IOrderService.ts
 *
 */

/// <reference path="../_references.ts" />

module IMCV.Koluki {
    "use strict";

    import IPromise = angular.IPromise;

    /**
     * Order service.
     *
     */
    export interface IOrderService {

        /**
         * Submits an order request.
         *
         * @param {OrderRequest} request Order request to send.
         * @returns {OrderResponse} Order response.
         */
        order(request: OrderRequest): IPromise<OrderResponse>;

    }

}