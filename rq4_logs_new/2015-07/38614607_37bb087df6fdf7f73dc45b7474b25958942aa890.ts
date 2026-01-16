/// <reference path="../../typings/tsd.d.ts" />
angular.module('ngRouteDemoApp').controller('listingController', ['ListingService', '$location', function (listingService, $location) {
    var vm = this

    vm.searchTerm = $location.search()['term'];

    vm.listings = [];
    console.log('listingController.searchTerm: ', vm.searchTerm);
    //console.log('getFeaturedListings: ', listingService.getFeaturedListings);
    //console.log('getFeaturedListings.results: ', listingService.getFeaturedListings());
    listingService.getSearchListings(vm.searchTerm).then(function(listings) {
        console.log("search listings: ", listings);
        if (listings) {
            vm.listings = listings;
            console.log('search listings: ', vm.listings);
        }
    });

}]);