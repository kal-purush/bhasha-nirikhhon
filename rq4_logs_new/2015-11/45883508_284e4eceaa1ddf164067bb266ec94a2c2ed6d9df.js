'use strict';

angular.module('myApp.view3', ['ngRoute'])

.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/view3', {
    templateUrl: 'view3/view3.html',
    controller: 'View3Ctrl'
  });
}])

angular.module('myApp.view3').directive('angularReel', ['$timeout',
  function($timeout) {
    'use strict';
    return {
      restrict: 'AC',
      link: function(scope, element, attrs) {
        var reelImages = attrs.reelImages;

        var imgLoad = imagesLoaded(element[0], function() {
          $(element[0]).reel({
            images: reelImages
          });

          if (!scope.$$phase) {
            scope.$apply();
          }
        });
      }
    };
  }
])


.controller('View3Ctrl', ['$scope', function($scope) {
  var items = [{
    'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
    'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    },
    {
      'image':'/test-angular-app/app/view1/fish/DSCN0691.JPG',
      'images': '/test-angular-app/app/view1/fish/DSCN####.JPG|691..702'
    }];

  $scope.items = items;
  $scope.imgLoadedEvents = {
    always: function(instance) {
      for(var i=0;i<items.length; i++)
      {
        $('#image-' + i ).reel({
          images: items[i].images
        });
      }
      $scope.all_images_loaded = true;
    },
    
    done: function(instance) {
      console.log('done');
    },

    fail: function(instance) {
      // Do stuff
      console.log('fail');
    }
  };
}]);