var rtfmApp = angular.module("rtfmApp", ['ui.router', 'firebase']);

rtfmApp.constant('fb', {
    url: 'test-app-lukey123948.firebaseIO.com'
});

rtfmApp.config(function ($stateProvider, $httpProvider, $urlRouterProvider) {

    $stateProvider

        .state('threads', {
        url: '/threads',
        templateUrl: 'threads.html',
        controller: 'threadsCtrl.js',
        resolve: {
            threadsRef: function (threadService) {
                return threadService.getThreads();
            }
        }
    })

    .state('thread', {
        url: '/threads/:threadId',
        templateUrl: 'path/to/thread.html',
        controller: 'threadCtrl',
        resolve: {
            threadRef: function (threadService, $stateParams) {
                return threadService.getThread($stateParams.threadId);
            },
            commentsRef: function (threadService, $stateParams) {
                return threadService.getComments($stateParams.threadId);
            }
        }
    });

    $urlRouterProvider.otherwise('/')
})