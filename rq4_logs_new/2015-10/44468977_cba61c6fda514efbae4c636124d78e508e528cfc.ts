/// <reference path="../typings/angular-material/angular-material.d.ts"/>

module SyncPlaylist {

    class SyncPlaylistController {
      public static $inject = [];
    }

    angular.module('syncPlaylist.main', ['ngMaterial', 'syncPlaylist.directives'])
        .config(['$mdThemingProvider', function($mdThemingProvider: ng.material.IThemingProvider) {
            $mdThemingProvider.theme('default')
                .primaryPalette('teal')
                .accentPalette('orange');
        }])
        .controller('SyncPlaylistController', SyncPlaylistController);
}