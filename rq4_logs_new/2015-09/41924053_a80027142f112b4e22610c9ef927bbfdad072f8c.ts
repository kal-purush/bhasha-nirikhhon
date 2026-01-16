/// <reference path="../../typings/tsd.d.ts" />
/// <reference path="../typings/fetch.d.ts" />

import 'corejs';
import angular from 'ionic';

import {DashCtrl, ChatsCtrl, ChatDetailCtrl, AccountCtrl, SigninCtrl} from 'controllers';
import {Chats, Data} from 'services';

angular.module('chat', ['ionic'])
  .service('Chats', Chats)
  .service('Data', Data)
  .controller('DashCtrl', DashCtrl)
  .controller('ChatsCtrl', ChatsCtrl)
  .controller('ChatDetailCtrl', ChatDetailCtrl)
  .controller('AccountCtrl', AccountCtrl)
  .controller('SigninCtrl', SigninCtrl)

  .constant('ApiUrl', 'http://localhost:3000')

  .config((
    $stateProvider: ng.ui.IStateProvider,
    $urlRouterProvider: ng.ui.IUrlRouterProvider,
    $httpProvider: ng.IHttpProvider
  ) => {
    $httpProvider.defaults.withCredentials = true;

    $stateProvider
      .state('signin', {
        url: '/signin',
        templateUrl: 'templates/signin.html',
        controller: 'SigninCtrl as signin',
        onEnter: ($state: ng.ui.IStateService, Data: Data) => {
          if(Data.has('user')) {
            $state.go('tab.dash');
          }
        }
      })

      .state('tab', {
        url: '/tab',
        abstract: true,
        templateUrl: 'templates/tabs.html',
        onEnter: ($state: ng.ui.IStateService, Data: Data) => {
          if(!Data.has('user')) {
            $state.go('signin');
          }
        }
      })

      .state('tab.dash', {
        url: '/dash',
        views: {
          'tab-dash': {
            templateUrl: 'templates/tab-dash.html',
            controller: 'DashCtrl as dash'
          }
        }
      })

      .state('tab.chats', {
        url: '/chats',
        views: {
          'tab-chats': {
            templateUrl: 'templates/tab-chats.html',
            controller: 'ChatsCtrl as chats'
          }
        }
      })

      .state('tab.chat-detail', {
        url: '/chats/:chatId',
        views: {
          'tab-chats': {
            templateUrl: 'templates/chat-detail.html',
            controller: 'ChatDetailCtrl as chatDetail'
          }
        }
      })

      .state('tab.account', {
        url: '/account',
        views: {
          'tab-account': {
            templateUrl: 'templates/tab-account.html',
            controller: 'AccountCtrl as account'
          }
        }
      });

    $urlRouterProvider.otherwise('/tab/dash');
  })

  .run(($ionicPlatform: ionic.platform.IonicPlatformService) => {
    $ionicPlatform.ready(() => {
      if(window.StatusBar) {
        window.StatusBar.styleLightContent();
      }
    });
  });

angular.bootstrap(window.document.body, ['chat']);