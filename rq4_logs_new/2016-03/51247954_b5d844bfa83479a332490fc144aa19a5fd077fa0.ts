/**
 * IPromotionRepository.ts
 *
 */

/// <reference path="../_references.ts" />

module IMCV.Koluki {
    "use strict";

    import IResource = angular.resource.IResource;
    import IResourceArray = angular.resource.IResourceArray;

    /**
     * Promotion repository.
     *
     */
    export interface IPromotionRepository {

        /**
         * Gets all promotions.
         *
         * @returns Collection of promotions.
         */
        getAllPromotions(): IResourceArray<IResource<Promotion>>;

    }

}