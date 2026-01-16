/**
 * ISurchargeRepository.ts
 *
 */

/// <reference path="../_references.ts" />

module IMCV.Koluki {
    "use strict";

    import IResource = angular.resource.IResource;

    /**
     * Surcharge repository.
     *
     */
    export interface ISurchargeRepository {

        /**
         * Gets delivery surcharge by suburb.
         *
         * @param {string} suburb Suburb to deliver to.
         * @returns Applicable delivery surcharge.
         */
        getDeliverySurchargeBySuburb(suburb: string): IResource<Surcharge>;

    }

}