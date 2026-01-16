// Ionic Starter App

// angular.module is a global place for creating, registering and retrieving Angular modules
// 'starter' is the name of this angular module example (also set in a <body> attribute in index.html)
// the 2nd parameter is an array of 'requires'
// 'starter.controllers' is found in controllers.js
angular.module('starter', ['ionic', 'starter.controllers'])

.run(function($ionicPlatform) {
  $ionicPlatform.ready(function() {
    // Hide the accessory bar by default (remove this to show the accessory bar above the keyboard
    // for form inputs)
    if (window.cordova && window.cordova.plugins.Keyboard) {
      cordova.plugins.Keyboard.hideKeyboardAccessoryBar(true);
      cordova.plugins.Keyboard.disableScroll(true);

    }
    if (window.StatusBar) {
      // org.apache.cordova.statusbar required
      StatusBar.styleDefault();
    }
  });
})

.config(function($stateProvider, $urlRouterProvider) {
  $stateProvider

    .state('app', {
    url: '/app',
    abstract: true,
    templateUrl: 'templates/menu.html',
    controller: 'AppCtrl'
  })

  .state('app.search', {
    url: '/search',
    views: {
      'menuContent': {
        templateUrl: 'templates/search.html',
        controller:'duyuruCtrl'
      }
    }
  })
  .state('app.kayitol', {
      url: '/kayitol',
      views: {
        'menuContent': {
          templateUrl: 'templates/kayitol.html',
          controller: 'KayitCtrl'
        }
      }
    })
    .state('app.faturaekle', {
        url: '/faturaekle',
        views: {
          'menuContent': {
            templateUrl: 'templates/faturaekle.html',
            controller: 'FaturaEkleCtrl'
          }
        }
      })

    .state('app.fatura', {
        url: '/fatura',
        views: {
          'menuContent': {
            templateUrl: 'templates/fatura.html',
            controller: 'FaturaCtrl'
          }
        }
      })
      .state('app.duyuruekle', {
          url: '/duyuruekle',
          views: {
            'menuContent': {
              templateUrl: 'templates/duyuruekle.html',
              controller: 'DuyuruEkle2Ctrl'
            }
          }
        })


      .state('app.hesap', {
          url: '/hesap',
          views: {
            'menuContent': {
              templateUrl: 'templates/hesap.html',
              controller: 'HesapCtrl'
            }
          }
        })

        .state('app.ödeme', {
            url: '/ödeme',
            views: {
              'menuContent': {
                templateUrl: 'templates/ödemeler.html',
                controller: 'ÖdemelerCtrl'
              }
            }
          })
          .state('app.sikayet', {
              url: '/sikayet',
              views: {
                'menuContent': {
                  templateUrl: 'templates/sikayet.html',
                  controller: 'SikayetCtrl'
                }
              }
            })
            .state('app.sikayetekle', {
                url: '/sikayetekle',
                views: {
                  'menuContent': {
                    templateUrl: 'templates/sikayetekle.html',
                    controller: 'SikayetekleCtrl'
                  }
                }
              })

            .state('app.menu2', {
                url: '/menu2',
                views: {
                  'menuContent': {
                    templateUrl: 'templates/menu2.html',
                    controller: 'Menu2Ctrl'
                  }
                }
              })

    .state('app.playlist', {
        url: '/playlist',
        views: {
          'menuContent': {
            templateUrl: 'templates/playlist.html',

          }
        }
      })



  .state('app.browse', {
      url: '/browse',
      views: {
        'menuContent': {
          templateUrl: 'templates/browse.html'
        }
      }
    })
    .state('app.playlists', {
      url: '/playlists',
      views: {
        'menuContent': {
          templateUrl: 'templates/playlists.html',
          controller: 'PlaylistsCtrl'
        }
      }
    })

  .state('app.single', {
    url: '/playlists/:playlistId',
    views: {
      'menuContent': {
        templateUrl: 'templates/playlist.html',
        controller: 'PlaylistCtrl'
      }
    }
  });
  // if none of the above states are matched, use this as the fallback
  $urlRouterProvider.otherwise('/app/playlists');
});